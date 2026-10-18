//! Opt-in, single-session receipts issued by the actual standard user consumer.
//!
//! This endpoint is deliberately concrete. Registration is not acceptance and
//! a context append is not an effects receipt. It never owns a shadow transcript.
use crate::{realtime::RealtimeState, SharedContext};
use pipecat_core::{
    Direction, Frame, FrameData, FrameObservationToken, ObservedProcessor, ProcessorContext,
    ProcessorError,
};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::oneshot;

const MAX_IDENTITIES: usize = 256;
const MAX_FINAL_BYTES: usize = 256 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;
static SESSION: AtomicU64 = AtomicU64::new(1);

fn error(message: impl Into<String>) -> ProcessorError {
    ProcessorError::fatal(message)
}

#[derive(Clone)]
pub(crate) struct Seal {
    token: FrameObservationToken,
    bytes: usize,
    direction: Direction,
    id: u64,
    name: String,
    pts: Option<u64>,
    metadata: BTreeMap<String, serde_json::Value>,
    transport_source: Option<String>,
    transport_destination: Option<String>,
    broadcast_sibling_id: Option<u64>,
    interruptible: bool,
    data: FrameData,
}
fn add_bytes(total: &mut usize, bytes: usize) -> Result<(), ProcessorError> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| error("history seal byte counter exhausted"))?;
    if *total > MAX_SNAPSHOT_BYTES {
        return Err(error("history sealed payload exceeds 1048576 bytes"));
    }
    Ok(())
}
fn bounded_value(
    value: &serde_json::Value,
    bytes: &mut usize,
    depth: usize,
) -> Result<(), ProcessorError> {
    if depth > 16 {
        return Err(error("history JSON nesting exceeds 16"));
    }
    add_bytes(bytes, 16)?;
    match value {
        serde_json::Value::String(value) => add_bytes(bytes, value.len())?,
        serde_json::Value::Array(values) => {
            if values.len() > 4096 {
                return Err(error("history JSON array exceeds 4096 elements"));
            }
            for value in values {
                bounded_value(value, bytes, depth + 1)?;
            }
        }
        serde_json::Value::Object(values) => {
            if values.len() > 4096 {
                return Err(error("history JSON object exceeds 4096 entries"));
            }
            for (key, value) in values {
                add_bytes(bytes, key.len())?;
                bounded_value(value, bytes, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn bounded_optional(value: &Option<String>, bytes: &mut usize) -> Result<(), ProcessorError> {
    if let Some(value) = value {
        add_bytes(bytes, value.len())?;
    }
    Ok(())
}
pub(crate) fn bounded_context(
    context: &pipecat_core::LlmContextData,
) -> Result<usize, ProcessorError> {
    if context.messages.len() > 4096
        || context.tool_choice.is_some()
        || !matches!(&context.tools, pipecat_core::ToolSet::NotGiven)
            && !matches!(&context.tools, pipecat_core::ToolSet::Functions(tools) if tools.is_empty())
    {
        return Err(error(
            "selected local context is not bounded tool-free history",
        ));
    }
    let mut bytes = 128;
    for message in &context.messages {
        if !message.tool_calls.is_empty() || message.extra.len() > 64 {
            return Err(error(
                "selected local context contains tools or excessive extensions",
            ));
        }
        add_bytes(&mut bytes, 64)?;
        bounded_optional(&message.name, &mut bytes)?;
        bounded_optional(&message.tool_call_id, &mut bytes)?;
        for (key, value) in &message.extra {
            add_bytes(&mut bytes, key.len())?;
            bounded_value(value, &mut bytes, 0)?;
        }
        match &message.content {
            pipecat_core::MessageContent::Text(text) => add_bytes(&mut bytes, text.len())?,
            pipecat_core::MessageContent::Parts(parts) => {
                if parts.len() > 4096 {
                    return Err(error("history message parts exceed 4096"));
                }
                for part in parts {
                    add_bytes(&mut bytes, 32)?;
                    match part {
                        pipecat_core::MessagePart::Text { text }
                        | pipecat_core::MessagePart::Refusal { text } => {
                            add_bytes(&mut bytes, text.len())?
                        }
                        pipecat_core::MessagePart::GeminiText {
                            text, signature, ..
                        }
                        | pipecat_core::MessagePart::Thinking { text, signature }
                        | pipecat_core::MessagePart::BedrockReasoning { text, signature } => {
                            add_bytes(&mut bytes, text.len())?;
                            bounded_optional(signature, &mut bytes)?;
                        }
                        _ => {
                            return Err(error(
                                "selected user history supports text-only local context",
                            ))
                        }
                    }
                }
            }
        }
    }
    Ok(bytes)
}
fn bounded_frame(frame: &Frame) -> Result<usize, ProcessorError> {
    if frame.native_speculation().is_some()
        || frame.native_eager_source().is_some()
        || frame.native_tool_batch()?.is_some()
        || frame.native_tool_outcome()?.is_some()
    {
        return Err(error(
            "selected history excludes native eager/speculative/tool carriers",
        ));
    }
    frame
        .require_no_native_llm_continuation()
        .map_err(|e| error(e.to_string()))?;
    if frame
        .native_user_image_constraint(Direction::Downstream)?
        .is_some()
        || frame
            .native_user_image_context(Direction::Downstream)?
            .is_some()
    {
        return Err(error("selected history excludes native image carriers"));
    }
    let native_context = frame.native_realtime_context_cause()?;
    if native_context.is_some()
        && !matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        )
    {
        return Err(error(
            "selected input cannot carry native context authority",
        ));
    }
    let mut bytes = 128;
    add_bytes(&mut bytes, frame.name.len())?;
    bounded_optional(&frame.transport_source, &mut bytes)?;
    bounded_optional(&frame.transport_destination, &mut bytes)?;
    if frame.metadata.len() > 64 {
        return Err(error("history metadata exceeds 64 keys"));
    }
    for (key, value) in &frame.metadata {
        add_bytes(&mut bytes, key.len())?;
        bounded_value(value, &mut bytes, 0)?;
    }
    match &frame.data {
        FrameData::Transcription(text) => {
            if text.text.len() > 64 * 1024 {
                return Err(error("one final transcript exceeds 65536 bytes"));
            }
            for value in [&text.text, &text.user_id, &text.timestamp] {
                add_bytes(&mut bytes, value.len())?;
            }
            bounded_optional(&text.language, &mut bytes)?;
            if let Some(value) = &text.result {
                bounded_value(value, &mut bytes, 0)?;
            }
        }
        FrameData::TranscriptionDetailed(text) => {
            if text.text.text.len() > 64 * 1024 {
                return Err(error("one final transcript exceeds 65536 bytes"));
            }
            for value in [&text.text.text, &text.user_id, &text.timestamp] {
                add_bytes(&mut bytes, value.len())?;
            }
            bounded_optional(&text.language, &mut bytes)?;
            if let Some(value) = &text.result {
                bounded_value(value, &mut bytes, 0)?;
            }
        }
        FrameData::Flush(flush) => bounded_optional(&flush.origin, &mut bytes)?,
        FrameData::LlmContext(context) => {
            if context.messages.len() > 4096
                || !context.tools.is_empty()
                || context.tool_choice.is_some()
            {
                return Err(error(
                    "selected local context is not bounded tool-free history",
                ));
            }
            for message in &context.messages {
                if !message.tool_calls.is_empty() {
                    return Err(error("selected local context contains tool calls"));
                }
                add_bytes(&mut bytes, 64 + message.content.len())?;
                bounded_optional(&message.name, &mut bytes)?;
                bounded_optional(&message.tool_call_id, &mut bytes)?;
            }
        }
        FrameData::LlmContextDetailed(context) => {
            add_bytes(&mut bytes, bounded_context(&context.context)?)?;
        }
        _ => {
            return Err(error(
                "registered history frame changed to an unsupported payload family",
            ))
        }
    }
    Ok(bytes)
}
impl Seal {
    pub(crate) fn new(frame: &Frame, direction: Direction) -> Result<Self, ProcessorError> {
        let bytes = bounded_frame(frame)?;
        Ok(Self {
            token: frame.observation_token(),
            bytes,
            direction,
            id: frame.id,
            name: frame.name.clone(),
            pts: frame.pts,
            metadata: frame.metadata.clone(),
            transport_source: frame.transport_source.clone(),
            transport_destination: frame.transport_destination.clone(),
            broadcast_sibling_id: frame.broadcast_sibling_id,
            interruptible: frame.interruptible,
            data: frame.data.clone(),
        })
    }
    fn identity(&self, frame: &Frame) -> bool {
        self.token == frame.observation_token()
    }
    pub(crate) fn matches(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        if !self.identity(frame) {
            return Ok(false);
        }
        bounded_frame(frame)?;
        if direction != self.direction
            || self.id != frame.id
            || self.name != frame.name
            || self.pts != frame.pts
            || self.metadata != frame.metadata
            || self.transport_source != frame.transport_source
            || self.transport_destination != frame.transport_destination
            || self.broadcast_sibling_id != frame.broadcast_sibling_id
            || self.interruptible != frame.interruptible
            || self.data != frame.data
        {
            return Err(error("registered history frame seal changed"));
        }
        Ok(true)
    }
}
#[cfg(test)]
pub(crate) struct HistoryTestPause {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) resume: tokio::sync::Notify,
}
#[cfg(test)]
impl HistoryTestPause {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        })
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Open,
    DrainingEnd,
    ClosingTerminal,
    Closed,
}
pub(crate) struct Registration {
    seal: Seal,
    processing: bool,
    pub(crate) staged: bool,
    accepted: bool,
    rejected: bool,
}
pub(crate) struct Ledger {
    pub(crate) seal: Seal,
    pub(crate) receipt: CommittedUserPrefix,
    pub(crate) admitted: bool,
}
struct Command {
    id: u64,
    through: u64,
    sender: oneshot::Sender<Result<CommittedUserPrefix, ProcessorError>>,
}
pub(crate) struct HistoryState {
    pub(crate) session: u64,
    pub(crate) phase: Phase,
    pub(crate) gate: Arc<Mutex<bool>>,
    pub(crate) lineage: u64,
    pub(crate) gate_setup: Option<ProcessorContext>,
    pub(crate) service_setup: Option<ProcessorContext>,
    pub(crate) assistant_setup: Option<ObservedProcessor>,
    pub(crate) started: bool,
    pub(crate) registrations: BTreeMap<u64, Registration>,
    pub(crate) next_ordinal: u64,
    accepted_through: u64,
    pub(crate) appended_through: u64,
    pub(crate) sequence: u64,
    pub(crate) ledger: VecDeque<Ledger>,
    command: Option<Command>,
    next_command: u64,
    failure: Option<String>,
    marker: Option<(Seal, bool, bool)>,
    #[cfg(test)]
    receipt_paused: bool,
    #[cfg(test)]
    snapshot_pause: Option<Arc<HistoryTestPause>>,
    #[cfg(test)]
    timer_pause: Option<Arc<HistoryTestPause>>,
    #[cfg(test)]
    consumer_pause: Option<Arc<HistoryTestPause>>,
    #[cfg(test)]
    staged_pause: Option<Arc<HistoryTestPause>>,
}
impl HistoryState {
    pub(crate) fn new(lineage: u64, gate: Arc<Mutex<bool>>) -> Result<Self, ProcessorError> {
        let session = SESSION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| error("history session identity exhausted"))?;
        Ok(Self {
            session,
            phase: Phase::Open,
            gate,
            lineage,
            gate_setup: None,
            service_setup: None,
            assistant_setup: None,
            started: false,
            registrations: BTreeMap::new(),
            next_ordinal: 0,
            accepted_through: 0,
            appended_through: 0,
            sequence: 0,
            ledger: VecDeque::new(),
            command: None,
            next_command: 0,
            failure: None,
            marker: None,
            #[cfg(test)]
            receipt_paused: false,
            #[cfg(test)]
            snapshot_pause: None,
            #[cfg(test)]
            timer_pause: None,
            #[cfg(test)]
            consumer_pause: None,
            #[cfg(test)]
            staged_pause: None,
        })
    }
    pub(crate) fn poison_setup(&mut self, message: &str) {
        self.failure.get_or_insert_with(|| message.into());
        self.phase = Phase::ClosingTerminal;
    }
    pub(crate) fn require_healthy(&self) -> Result<(), ProcessorError> {
        if let Some(message) = &self.failure {
            return Err(error(message.clone()));
        }
        Ok(())
    }
    pub(crate) fn require_open(&self) -> Result<(), ProcessorError> {
        self.require_healthy()?;
        if matches!(self.phase, Phase::ClosingTerminal | Phase::Closed) {
            return Err(error("user history session releases are closed"));
        }
        Ok(())
    }
    fn compact(&mut self) {
        self.registrations.retain(|ordinal, entry| {
            *ordinal > self.appended_through
                || *ordinal > self.accepted_through
                || entry.seal.token.is_alive()
        });
        // Keep the most recent receipt for old-prefix requests. Older identities
        // remain valid only while an actual native frame still exists.
        let last = self.ledger.back().map(|entry| entry.receipt.sequence);
        self.ledger.retain(|entry| {
            Some(entry.receipt.sequence) == last || !entry.admitted || entry.seal.token.is_alive()
        });
    }
    pub(crate) fn prepare_ledger(
        &mut self,
        frame: &Frame,
        through: u64,
    ) -> Result<Ledger, ProcessorError> {
        self.compact();
        if self.ledger.len() >= MAX_IDENTITIES {
            return Err(error("local history snapshot identity capacity exhausted"));
        }
        let seal = Seal::new(frame, Direction::Downstream)?;
        let bytes = self
            .ledger
            .iter()
            .try_fold(seal.bytes, |n, record| n.checked_add(record.seal.bytes))
            .ok_or_else(|| error("history snapshot byte counter exhausted"))?;
        if bytes > MAX_SNAPSHOT_BYTES {
            return Err(error(
                "retained local history snapshots exceed 1048576 bytes",
            ));
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| error("user commit sequence exhausted"))?;
        Ok(Ledger {
            seal,
            receipt: CommittedUserPrefix {
                session: self.session,
                through,
                sequence,
                lineage: self.lineage,
            },
            admitted: false,
        })
    }
    pub(crate) fn staged_through(&self) -> Result<u64, ProcessorError> {
        let mut through = self.appended_through;
        for (ordinal, entry) in self.registrations.range((
            std::ops::Bound::Excluded(through),
            std::ops::Bound::Unbounded,
        )) {
            if !entry.staged {
                break;
            }
            if *ordinal
                != through
                    .checked_add(1)
                    .ok_or_else(|| error("history ordinal exhausted"))?
            {
                return Err(error("registered history prefix has a gap"));
            }
            through = *ordinal;
        }
        Ok(through)
    }
    pub(crate) fn stage(&mut self, frame: &Frame) -> Result<(), ProcessorError> {
        self.require_healthy()?;
        if self.phase == Phase::Closed {
            return Err(error("history append consumer is closed"));
        }
        // A real consumer claimed while Open may finish its append policy
        // during Stop's ClosingTerminal; new claims are retired at entry.
        let ordinal = self
            .registrations
            .iter()
            .find(|(_, entry)| entry.seal.identity(frame))
            .map(|(ordinal, _)| *ordinal)
            .ok_or_else(|| error("actual history append has no registration"))?;
        let expected = self
            .staged_through()?
            .checked_add(1)
            .ok_or_else(|| error("staged history ordinal exhausted"))?;
        if ordinal != expected {
            return Err(error("registered history final arrived with a prefix gap"));
        }
        let entry = self
            .registrations
            .get_mut(&ordinal)
            .expect("selected exact registration");
        if !entry.processing || entry.staged {
            return Err(error("history append has no live fresh processing guard"));
        }
        entry.staged = true;
        Ok(())
    }
    pub(crate) fn command_ready(&self) -> Result<bool, ProcessorError> {
        self.require_open()?;
        Ok(self.command.as_ref().is_some_and(|command| {
            self.accepted_through >= command.through && self.appended_through < command.through
        }))
    }
}

/// Proof of an actual user append and successful local snapshot queue admission.
/// Only the bound standard user/assistant flush machinery can construct it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedUserPrefix {
    session: u64,
    through: u64,
    sequence: u64,
    lineage: u64,
}
impl CommittedUserPrefix {
    pub fn session_id(&self) -> u64 {
        self.session
    }
    pub fn through(&self) -> u64 {
        self.through
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn context_lineage(&self) -> u64 {
        self.lineage
    }
    pub fn effects_admitted(&self) -> bool {
        true
    }
}

/// One pending, revocable commit command. Dropping it never rolls history back.
pub struct UserHistoryCommit {
    endpoint: RealtimeUserHistoryEndpoint,
    id: u64,
    receiver: Option<oneshot::Receiver<Result<CommittedUserPrefix, ProcessorError>>>,
}
impl UserHistoryCommit {
    pub async fn wait(mut self) -> Result<CommittedUserPrefix, ProcessorError> {
        let receipt = self
            .receiver
            .take()
            .expect("single history receipt wait")
            .await
            .map_err(|_| error("user history receipt owner closed"))??;
        self.endpoint.validate_receipt(&receipt)?;
        Ok(receipt)
    }
}
impl Drop for UserHistoryCommit {
    fn drop(&mut self) {
        let revoked = if let Ok(mut state) = self.endpoint.realtime.lock() {
            state.history.as_mut().and_then(|history| {
                if history
                    .command
                    .as_ref()
                    .is_some_and(|command| command.id == self.id)
                {
                    history.command.take()
                } else {
                    None
                }
            })
        } else {
            None
        };
        drop(revoked);
        self.endpoint.realtime.changed.notify_one();
    }
}

/// Once-reserved, opaque connection between one realtime pair and its producer.
#[derive(Clone)]
pub struct RealtimeUserHistoryEndpoint {
    pub(crate) realtime: Arc<RealtimeState>,
    pub(crate) context: SharedContext,
    pub(crate) gate: Arc<Mutex<bool>>,
}
impl RealtimeUserHistoryEndpoint {
    /// Bounded lossless plain-text seed projection. Validation precedes clones.
    pub fn initial_seed(&self) -> Result<pipecat_core::LlmContextFrame, ProcessorError> {
        let state = self.realtime.lock()?;
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        let context = self.context.lock()?;
        if context.lineage != history.lineage {
            return Err(error("history seed context lineage changed"));
        }
        bounded_context(&context)?;
        if context.messages.iter().any(|message| {
            !message.extra.is_empty()
                || !matches!(&message.content, pipecat_core::MessageContent::Text(_))
        }) {
            return Err(error("history seed requires lossless plain legacy text"));
        }
        context.legacy_frame()
    }
    pub fn session_id(&self) -> Result<u64, ProcessorError> {
        Ok(self
            .realtime
            .lock()?
            .history
            .as_ref()
            .ok_or_else(|| error("history endpoint is unbound"))?
            .session)
    }
    pub fn bind_gate_setup(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.bind_setup(ctx, true)
    }
    pub fn bind_service_setup(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.bind_setup(ctx, false)
    }
    fn bind_setup(&self, ctx: &ProcessorContext, gate: bool) -> Result<(), ProcessorError> {
        let duplicate = {
            let mut state = self.realtime.lock()?;
            let history = state
                .history
                .as_mut()
                .ok_or_else(|| error("history endpoint is unbound"))?;
            history.require_open()?;
            if history.started
                || if gate {
                    history.gate_setup.is_some()
                } else {
                    history.service_setup.is_some()
                }
            {
                history.poison_setup("history selected component setup cannot be repeated");
                true
            } else {
                if gate {
                    history.gate_setup = Some(ctx.clone());
                } else {
                    history.service_setup = Some(ctx.clone());
                }
                false
            }
        };
        if duplicate {
            self.realtime
                .fail_history("history selected component setup cannot be repeated", true)?;
            return Err(error("history selected component setup cannot be repeated"));
        }
        Ok(())
    }
    /// Validate all four successful selected setups before opening the provider.
    pub fn validate_start(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.realtime.lock()?;
        let user = state
            .user_identity
            .clone()
            .ok_or_else(|| error("paired user setup did not succeed"))?;
        let history = state
            .history
            .as_mut()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        if history.started {
            return Err(error("history route cannot restart"));
        }
        let gate = history
            .gate_setup
            .as_ref()
            .ok_or_else(|| error("history ingress gate setup did not succeed"))?
            .processor_identity();
        let service = history
            .service_setup
            .as_ref()
            .ok_or_else(|| error("history service setup did not succeed"))?
            .processor_identity();
        let assistant = history
            .assistant_setup
            .as_ref()
            .ok_or_else(|| error("paired assistant setup did not succeed"))?;
        if service != ctx.processor_identity()
            || gate.runtime_id != user.runtime_id
            || service.runtime_id != user.runtime_id
            || assistant.runtime_id != user.runtime_id
            || gate.index.checked_add(1) != Some(user.index)
            || user.index.checked_add(1) != Some(service.index)
            || service.index >= assistant.index
        {
            return Err(error("history route requires exact contiguous gate/user/service and downstream paired assistant in one runtime"));
        }
        if self.context.lock()?.lineage != history.lineage {
            return Err(error("history context lineage changed before start"));
        }
        history.started = true;
        Ok(())
    }
    pub fn register_final(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<u64, ProcessorError> {
        let valid = match &frame.data {
            FrameData::Transcription(text) => text.finalized && !text.text.trim().is_empty(),
            FrameData::TranscriptionDetailed(text) => {
                text.finalized && text.text.append_to_context && !text.text.text.trim().is_empty()
            }
            _ => false,
        };
        if !valid || direction != Direction::Upstream || frame.interruptible {
            return Err(error("history registration requires a nonempty finalized noninterruptible upstream transcription"));
        }
        let seal = Seal::new(frame, direction)?;
        let mut state = self.realtime.lock()?;
        let history = state
            .history
            .as_mut()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        if !history.started {
            return Err(error("history producer has not passed startup binding"));
        }
        for (ordinal, entry) in &history.registrations {
            if entry.seal.matches(frame, direction)? {
                return Ok(*ordinal);
            }
        }
        history.compact();
        if history.registrations.len() >= MAX_IDENTITIES {
            return Err(error("pending exact history identities exceed 256"));
        }
        let bytes = history
            .registrations
            .values()
            .try_fold(seal.bytes, |n, entry| n.checked_add(entry.seal.bytes))
            .ok_or_else(|| error("history final byte counter exhausted"))?;
        if bytes > MAX_FINAL_BYTES {
            return Err(error("retained final history text exceeds 262144 bytes"));
        }
        let ordinal = history
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| error("final history ordinal exhausted"))?;
        history.next_ordinal = ordinal;
        history.registrations.insert(
            ordinal,
            Registration {
                seal,
                processing: false,
                staged: false,
                accepted: false,
                rejected: false,
            },
        );
        Ok(ordinal)
    }
    pub fn commit_through(&self, through: u64) -> Result<UserHistoryCommit, ProcessorError> {
        let (sender, receiver) = oneshot::channel();
        let id = {
            let mut state = self.realtime.lock()?;
            let history = state
                .history
                .as_mut()
                .ok_or_else(|| error("history endpoint is unbound"))?;
            history.require_open()?;
            if through == 0 || through > history.next_ordinal {
                return Err(error("history commit requests an unregistered prefix"));
            }
            if history.command.is_some() {
                return Err(error("only one user history commit may be pending"));
            }
            let id = history
                .next_command
                .checked_add(1)
                .ok_or_else(|| error("history command identity exhausted"))?;
            history.next_command = id;
            history.command = Some(Command {
                id,
                through,
                sender,
            });
            id
        };
        self.realtime.changed.notify_one();
        self.realtime.resolve_history(&self.context)?;
        Ok(UserHistoryCommit {
            endpoint: self.clone(),
            id,
            receiver: Some(receiver),
        })
    }
    pub fn validate_receipt(&self, receipt: &CommittedUserPrefix) -> Result<(), ProcessorError> {
        let state = self.realtime.lock()?;
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        if receipt.session != history.session
            || receipt.through > history.accepted_through
            || receipt.through > history.appended_through
            || receipt.sequence > history.sequence
            || self.context.lock()?.lineage != receipt.lineage
        {
            return Err(error(
                "user history receipt is foreign or its context lineage changed",
            ));
        }
        Ok(())
    }
    /// Admit plain mapped service output, or an exact registered final on the
    /// session setup route. False means release closed or the original output
    /// scope retired. Downstream local executable payloads are never supported.
    pub fn admit(
        &self,
        ctx: &ProcessorContext,
        frame: Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        if direction == Direction::Downstream {
            // Reject opaque native carriers before taking the release gate;
            // stale runtime admission must not drop their destructors here.
            frame
                .require_no_native_llm_continuation()
                .map_err(|e| error(e.to_string()))?;
            // Keep this concrete endpoint limited to the selected provider's
            // plain mapper output. A negative control list cannot exclude all
            // LocalObject/ProcessorReference payloads whose last-owner Drop
            // might reenter this gate when runtime enqueue rejects the frame.
            if !matches!(
                frame.data,
                FrameData::LlmResponseStart
                    | FrameData::LlmResponseEnd
                    | FrameData::LlmThoughtStart
                    | FrameData::LlmThoughtEnd
                    | FrameData::LlmThoughtText(_)
                    | FrameData::LlmText(_)
                    | FrameData::TtsStarted(_)
                    | FrameData::TtsStopped(_)
                    | FrameData::TtsAudio(_)
                    | FrameData::TtsText(_)
                    | FrameData::InterimTranscription(_)
                    | FrameData::Metrics(_)
            ) {
                return Err(error(
                    "history service admission requires plain mapped output",
                ));
            }
        }
        let setup = {
            let state = self.realtime.lock()?;
            let history = state
                .history
                .as_ref()
                .ok_or_else(|| error("history endpoint is unbound"))?;
            let setup = history
                .service_setup
                .as_ref()
                .ok_or_else(|| error("selected service setup is unavailable"))?;
            if ctx.processor_identity() != setup.processor_identity() {
                return Err(error(
                    "history admission came from a foreign service runtime",
                ));
            }
            if direction == Direction::Upstream {
                if !ctx.same_causal_scope(setup) {
                    return Err(error(
                        "final history admission requires the exact setup scope",
                    ));
                }
                let mut registered = false;
                for entry in history.registrations.values() {
                    registered |= entry.seal.matches(&frame, direction)?;
                }
                if !registered {
                    return Err(error("final history admission has no exact registration"));
                }
            }
            setup.clone()
        };
        let open = self
            .gate
            .lock()
            .map_err(|_| error("history release gate poisoned"))?;
        if !*open {
            return Ok(false);
        }
        if direction == Direction::Upstream {
            setup.try_push(frame, direction)?;
        } else {
            if !ctx.is_current() {
                return Ok(false);
            }
            ctx.try_push(frame, direction)?;
        }
        Ok(true)
    }
    /// Send the exact registered End fence using the gate's immutable setup route.
    pub fn admit_end_marker(&self, frame: &Frame) -> Result<bool, ProcessorError> {
        let setup = {
            let state = self.realtime.lock()?;
            let history = state
                .history
                .as_ref()
                .ok_or_else(|| error("history endpoint is unbound"))?;
            let Some((seal, false, false)) = &history.marker else {
                return Err(error("End marker is absent or already consumed"));
            };
            if !seal.matches(frame, Direction::Downstream)? {
                return Err(error("End marker identity mismatch"));
            }
            history
                .gate_setup
                .clone()
                .ok_or_else(|| error("history gate setup is unavailable"))?
        };
        let open = self
            .gate
            .lock()
            .map_err(|_| error("history release gate poisoned"))?;
        if !*open {
            return Ok(false);
        }
        setup.try_push(frame.clone(), Direction::Downstream)?;
        Ok(true)
    }
    pub fn begin_draining(&self) -> Result<(), ProcessorError> {
        let mut state = self.realtime.lock()?;
        let history = state
            .history
            .as_mut()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        if history.phase != Phase::Open {
            return Err(error("history End drain already started"));
        }
        history.phase = Phase::DrainingEnd;
        Ok(())
    }
    /// Revoke releases while retaining the standard buffer for terminal append.
    pub fn close_releases(&self) -> Result<(), ProcessorError> {
        *self
            .gate
            .lock()
            .map_err(|_| error("history release gate poisoned"))? = false;
        self.realtime
            .fail_history("user history session closed", false)
    }
    pub fn close(&self) -> Result<(), ProcessorError> {
        self.close_releases()?;
        self.realtime.close()
    }
    pub fn accept_local_context(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        let state = self.realtime.lock()?;
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        // Validate known identities before dispatch by public payload family.
        for entry in history.registrations.values() {
            entry.seal.matches(frame, direction)?;
        }
        if let Some((seal, _, _)) = &history.marker {
            seal.matches(frame, direction)?;
        }
        for record in &history.ledger {
            if record.seal.matches(frame, direction)? {
                if matches!(history.phase, Phase::Open | Phase::DrainingEnd) {
                    history.require_healthy()?;
                    if self.context.lock()?.lineage != history.lineage {
                        return Err(error(
                            "history context lineage changed before local feedback",
                        ));
                    }
                }
                // Already-admitted local notifications remain consumable after
                // close; this is neither new feedback admission nor a receipt.
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn register_end_marker(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        if direction != Direction::Downstream
            || frame.interruptible
            || !matches!(&frame.data, FrameData::Flush(flush) if !flush.returning && flush.origin.as_deref() == Some("gemini_history_end_input"))
        {
            return Err(error("invalid history End input marker"));
        }
        let seal = Seal::new(frame, direction)?;
        let mut state = self.realtime.lock()?;
        let history = state
            .history
            .as_mut()
            .ok_or_else(|| error("history endpoint is unbound"))?;
        history.require_open()?;
        if history.phase != Phase::DrainingEnd || history.marker.is_some() {
            return Err(error("history End marker requires one active drain"));
        }
        history.marker = Some((seal, false, false));
        Ok(())
    }
    pub fn validate_end_marker(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        self.realtime.history_marker(frame, direction, false)
    }
}

pub(crate) fn validate_live_control(data: &FrameData) -> Result<(), ProcessorError> {
    if matches!(
        data,
        FrameData::InputText(_)
            | FrameData::LlmRun
            | FrameData::LlmMessagesAppend(_)
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesAppendDetailed(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_)
            | FrameData::LlmSetTools(_)
            | FrameData::LlmSetToolChoice(_)
            | FrameData::FunctionCallsStarted(_)
            | FrameData::FunctionCallsStartedDetailed(_)
            | FrameData::FunctionCallInProgress(_)
            | FrameData::FunctionCallResult(_)
            | FrameData::FunctionCallResultDetailed(_)
            | FrameData::FunctionCallCancel(_)
            | FrameData::ExternalFunctionCallsStarted(_)
            | FrameData::ExternalFunctionCallInProgress(_)
            | FrameData::ExternalFunctionCallResult(_)
            | FrameData::ExternalFunctionCallCancel(_)
            | FrameData::AssistantImage(_)
            | FrameData::LlmContinuationCheckpoint
            | FrameData::LlmContextSummaryRequest(_)
            | FrameData::LlmContextSummaryResult(_)
            | FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::UserImage(_)
            | FrameData::InputImage(_)
            | FrameData::UserImageRequest(_)
            | FrameData::InputFile(_)
            | FrameData::UserFile(_)
            | FrameData::UserFileDetailed(_)
            | FrameData::LlmSummarizeContext(_)
    ) {
        return Err(error(
            "selected user history excludes live context edits, tools, summaries and images",
        ));
    }
    Ok(())
}

pub(crate) fn transcription(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::Transcription(_)
            | FrameData::TranscriptionDetailed(_)
            | FrameData::InterimTranscription(_)
            | FrameData::InterimTranscriptionDetailed(_)
            | FrameData::EagerTranscription(_)
            | FrameData::EagerTranscriptionDetailed(_)
            | FrameData::Translation(_)
            | FrameData::TranslationDetailed(_)
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsumerDisposition {
    Processing,
    AcceptedClone,
    Retired,
}
pub(crate) struct Processing {
    realtime: Arc<RealtimeState>,
    ordinal: u64,
    finished: bool,
    disposition: ConsumerDisposition,
}
impl Processing {
    pub(crate) fn skip_processing(&self) -> bool {
        self.disposition != ConsumerDisposition::Processing
    }
    pub(crate) fn finish(mut self) -> Result<(), ProcessorError> {
        if self.skip_processing() {
            self.finished = true;
            return Ok(());
        }
        {
            let mut state = self.realtime.lock()?;
            let history = state
                .history
                .as_mut()
                .ok_or_else(|| error("history registration disappeared"))?;
            history.require_healthy()?;
            if history.phase == Phase::Closed {
                self.finished = true;
                return Ok(());
            }
            if history.phase != Phase::ClosingTerminal {
                history.require_open()?;
            }
            let entry = history
                .registrations
                .get_mut(&self.ordinal)
                .ok_or_else(|| error("history registration disappeared"))?;
            if !entry.staged || entry.rejected {
                return Err(error(
                    "registered final was not accepted by the actual user append policy",
                ));
            }
            entry.processing = false;
            entry.accepted = true;
            while history.accepted_through < history.next_ordinal {
                let next = history
                    .accepted_through
                    .checked_add(1)
                    .ok_or_else(|| error("accepted history ordinal exhausted"))?;
                if !history
                    .registrations
                    .get(&next)
                    .is_some_and(|entry| entry.accepted)
                {
                    break;
                }
                history.accepted_through = next;
            }
        }
        self.finished = true;
        self.realtime.changed.notify_one();
        Ok(())
    }
}
impl Drop for Processing {
    fn drop(&mut self) {
        if !self.finished && !self.skip_processing() {
            if let Ok(mut state) = self.realtime.lock() {
                if let Some(history) = &mut state.history {
                    if let Some(entry) = history.registrations.get_mut(&self.ordinal) {
                        entry.rejected = true;
                        entry.processing = false;
                    }
                }
            }
            let _ = self.realtime.fail_history(
                "registered user final processing failed or was dropped",
                true,
            );
        }
    }
}
impl RealtimeState {
    #[cfg(test)]
    pub(crate) async fn pause_history_staged(&self) {
        let pause = self.lock().ok().and_then(|mut state| {
            state
                .history
                .as_mut()
                .and_then(|history| history.staged_pause.take())
        });
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }
    #[cfg(test)]
    pub(crate) async fn pause_history_consumer(&self) {
        let pause = self.lock().ok().and_then(|mut state| {
            state
                .history
                .as_mut()
                .and_then(|history| history.consumer_pause.take())
        });
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }
    #[cfg(test)]
    pub(crate) async fn pause_history_effects(&self, snapshot: bool) {
        let pause = self.lock().ok().and_then(|mut state| {
            state.history.as_mut().and_then(|history| {
                if snapshot {
                    history.snapshot_pause.take()
                } else {
                    history.timer_pause.take()
                }
            })
        });
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause.resume.notified().await;
        }
    }
    pub(crate) fn history_validate_assistant_control(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        let state = self.lock()?;
        let Some(history) = &state.history else {
            return Ok(());
        };
        for entry in history.registrations.values() {
            entry.seal.matches(frame, direction)?;
        }
        if let Some((seal, _, _)) = &history.marker {
            seal.matches(frame, direction)?;
        }
        for record in &history.ledger {
            if record.seal.matches(frame, direction)? {
                return Ok(());
            }
        }
        // Registered finals travel directly from the service to the user. An
        // upstream transcript at this boundary is a foreign producer, and the
        // ordinary assistant reducer would otherwise consume it before the
        // selected service or user can reject it. Provider interim observations
        // travelling downstream remain harmless observations.
        if direction == Direction::Upstream && transcription(&frame.data) {
            return Err(error(
                "selected assistant rejects upstream transcription producers",
            ));
        }
        validate_live_control(&frame.data)
    }
    pub(crate) fn history_processing(
        self: &Arc<Self>,
        frame: &Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<Option<Processing>, ProcessorError> {
        let mut state = self.lock()?;
        let user = state.user_identity.clone();
        let Some(history) = &mut state.history else {
            return Ok(None);
        };
        let closing = matches!(history.phase, Phase::ClosingTerminal | Phase::Closed);
        if !closing
            && (transcription(&frame.data)
                || history
                    .registrations
                    .values()
                    .any(|entry| entry.seal.identity(frame)))
        {
            history.require_open()?;
        }
        for (ordinal, entry) in &mut history.registrations {
            if entry.seal.matches(frame, direction)? {
                if user.as_ref() != Some(&ctx.processor_identity()) || !ctx.is_usable() {
                    return Err(error(
                        "registered history consumer runtime or health is invalid",
                    ));
                }
                if closing {
                    return Ok(Some(Processing {
                        realtime: self.clone(),
                        ordinal: *ordinal,
                        finished: true,
                        disposition: ConsumerDisposition::Retired,
                    }));
                }
                if entry.rejected || entry.processing {
                    return Err(error(
                        "registered user final is rejected or concurrently processing",
                    ));
                }
                let duplicate = entry.accepted;
                entry.processing = !duplicate;
                return Ok(Some(Processing {
                    realtime: self.clone(),
                    ordinal: *ordinal,
                    finished: false,
                    disposition: if duplicate {
                        ConsumerDisposition::AcceptedClone
                    } else {
                        ConsumerDisposition::Processing
                    },
                }));
            }
        }
        if transcription(&frame.data) {
            return Err(error(
                "bound user rejects unregistered transcription producers",
            ));
        }
        Ok(None)
    }
    pub(crate) fn history_validate_reducer(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        let state = self.lock()?;
        let Some(history) = &state.history else {
            return Ok(false);
        };
        for entry in history.registrations.values() {
            if entry.seal.matches(frame, direction)? {
                history.require_healthy()?;
                if history.phase == Phase::Closed {
                    return Err(error("history reducer consumer is closed"));
                }
                if !entry.processing {
                    return Err(error(
                        "registered history requires the actual user processor invocation",
                    ));
                }
                return Ok(true);
            }
        }
        if transcription(&frame.data) {
            return Err(error(
                "bound user rejects unregistered transcription producers",
            ));
        }
        Ok(false)
    }
    pub(crate) fn history_marker(
        &self,
        frame: &Frame,
        direction: Direction,
        user: bool,
    ) -> Result<bool, ProcessorError> {
        let mut state = self.lock()?;
        let Some(history) = &mut state.history else {
            return Ok(false);
        };
        if let Some((seal, user_seen, service_seen)) = &mut history.marker {
            if seal.matches(frame, direction)? {
                if matches!(history.phase, Phase::ClosingTerminal | Phase::Closed) {
                    return Err(error("history End marker was revoked"));
                }
                if (user && *user_seen) || (!user && (*service_seen || !*user_seen)) {
                    return Err(error(
                        "history End marker replay or missing actual user boundary",
                    ));
                }
                if user {
                    *user_seen = true;
                } else {
                    *service_seen = true;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn resolve_history(&self, context: &SharedContext) -> Result<(), ProcessorError> {
        let notification = {
            let mut state = self.lock()?;
            let Some(history) = &mut state.history else {
                return Ok(());
            };
            #[cfg(test)]
            if history.receipt_paused {
                return Ok(());
            }
            let Some(command) = &history.command else {
                return Ok(());
            };
            if let Err(error) = history.require_open() {
                history
                    .command
                    .take()
                    .map(|command| (command.sender, Err(error)))
            } else if context.lock()?.lineage != history.lineage {
                history.command.take().map(|command| {
                    (
                        command.sender,
                        Err(error("history context lineage changed before receipt")),
                    )
                })
            } else {
                let receipt = history
                    .ledger
                    .iter()
                    .find(|record| {
                        record.admitted
                            && record.receipt.through >= command.through
                            && history.accepted_through >= record.receipt.through
                    })
                    .map(|record| record.receipt.clone());
                receipt.and_then(|receipt| {
                    history
                        .command
                        .take()
                        .map(|command| (command.sender, Ok(receipt)))
                })
            }
        };
        if let Some((sender, result)) = notification {
            let _ = sender.send(result);
        }
        Ok(())
    }
    pub(crate) fn fail_history(&self, message: &str, failed: bool) -> Result<(), ProcessorError> {
        let gate = {
            let state = self.lock()?;
            let Some(history) = &state.history else {
                return Ok(());
            };
            history.gate.clone()
        };
        *gate
            .lock()
            .map_err(|_| error("history release gate poisoned"))? = false;
        let notification = {
            let mut state = self.lock()?;
            let Some(history) = &mut state.history else {
                return Ok(());
            };
            if history.phase != Phase::Closed {
                history.phase = Phase::ClosingTerminal;
            }
            if failed {
                history.failure.get_or_insert_with(|| message.into());
            }
            history.command.take().map(|command| command.sender)
        };
        if let Some(sender) = notification {
            let _ = sender.send(Err(error(message)));
        }
        self.changed.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LLMContextAggregatorPair, RealtimeServiceMode, UserAggregatorConfig};
    use pipecat_core::{
        async_trait, FrameProcessor, Pipeline, PipelineWorker, StartFrame, TranscriptionFrame,
    };
    use pipecat_services::{Content, Context, Role};
    use std::time::Duration;
    use tokio::sync::mpsc;

    struct Gate(RealtimeUserHistoryEndpoint);
    #[async_trait]
    impl FrameProcessor for Gate {
        fn name(&self) -> &str {
            "history-test-gate"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.0.bind_gate_setup(ctx)
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(
                frame.data,
                FrameData::End(_) | FrameData::Stop | FrameData::Cancel(_)
            ) {
                self.0.close_releases()?;
            }
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.0.close()
        }
    }
    struct Service {
        endpoint: RealtimeUserHistoryEndpoint,
        setup: Mutex<Option<ProcessorContext>>,
        observed: mpsc::UnboundedSender<Frame>,
    }
    #[async_trait]
    impl FrameProcessor for Service {
        fn name(&self) -> &str {
            "history-test-service"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.endpoint.bind_service_setup(ctx)?;
            *self.setup.lock().unwrap() = Some(ctx.clone());
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if frame.data.is_start() && direction == Direction::Downstream {
                self.endpoint.validate_start(ctx)?;
            }
            if self.endpoint.validate_end_marker(&frame, direction)? {
                let _ = self.observed.send(frame);
                return Ok(());
            }
            if matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            ) {
                if !self.endpoint.accept_local_context(&frame, direction)? {
                    return Err(error("foreign context"));
                }
                let _ = self.observed.send(frame);
                return Ok(());
            }
            let _ = self.observed.send(frame.clone());
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.setup.lock().unwrap().take();
            self.endpoint.close()
        }
    }
    fn make_pair(config: UserAggregatorConfig) -> LLMContextAggregatorPair {
        LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            config,
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap()
    }
    async fn start(
        pair: &LLMContextAggregatorPair,
    ) -> (
        RealtimeUserHistoryEndpoint,
        Arc<Service>,
        PipelineWorker,
        mpsc::UnboundedReceiver<Frame>,
    ) {
        let endpoint = pair.reserve_realtime_user_history().unwrap();
        let (observed, mut observations) = mpsc::unbounded_channel();
        let service = Arc::new(Service {
            endpoint: endpoint.clone(),
            setup: Mutex::new(None),
            observed,
        });
        let worker = Pipeline::new(vec![
            Arc::new(Gate(endpoint.clone())),
            pair.user(),
            service.clone(),
            pair.assistant(),
        ])
        .start(Default::default())
        .await
        .unwrap();
        observed_frame(&mut observations, FrameData::is_start).await;
        (endpoint, service, worker, observations)
    }
    async fn observed_frame(
        observations: &mut mpsc::UnboundedReceiver<Frame>,
        wanted: impl Fn(&FrameData) -> bool,
    ) -> Frame {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let frame = observations.recv().await.expect("service observation");
                if wanted(&frame.data) {
                    return frame;
                }
            }
        })
        .await
        .expect("real processor observation deadline")
    }
    fn final_frame(text: &str) -> Frame {
        let mut frame = Frame::new(FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized: true,
            ..Default::default()
        }));
        frame.interruptible = false;
        frame
    }
    fn inject(endpoint: &RealtimeUserHistoryEndpoint, service: &Service, frame: &Frame) -> u64 {
        let ordinal = endpoint.register_final(frame, Direction::Upstream).unwrap();
        let setup = service.setup.lock().unwrap().clone().unwrap();
        assert!(endpoint
            .admit(&setup, frame.clone(), Direction::Upstream)
            .unwrap());
        ordinal
    }
    fn user_text(pair: &LLMContextAggregatorPair) -> Vec<String> {
        pair.context()
            .snapshot()
            .unwrap()
            .messages
            .into_iter()
            .filter(|message| message.role == Role::User)
            .map(|message| match message.content {
                Some(Content::Text(text)) => text,
                _ => panic!("text user history"),
            })
            .collect()
    }
    async fn finish(mut worker: PipelineWorker) {
        worker.end().await.unwrap();
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
    #[tokio::test]
    async fn opaque_service_output_is_rejected_and_dropped_outside_release_gate() {
        struct DropProbe {
            gate: Arc<Mutex<bool>>,
            outside_gate: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.outside_gate
                    .store(self.gate.try_lock().is_ok(), Ordering::SeqCst);
            }
        }
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, _) = start(&pair).await;
        let outside_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let frame = Frame::new(FrameData::ErrorDetailed(pipecat_core::DetailedErrorFrame {
            error: "unsupported local exception".into(),
            fatal: false,
            processor: None,
            exception: Some(pipecat_core::LocalObject::new(DropProbe {
                gate: endpoint.gate.clone(),
                outside_gate: outside_gate.clone(),
            })),
            category: None,
        }));
        let setup = service.setup.lock().unwrap().clone().unwrap();
        assert!(endpoint
            .admit(&setup, frame, Direction::Downstream)
            .is_err());
        assert!(outside_gate.load(Ordering::SeqCst));
        finish(worker).await;
    }
    #[tokio::test]
    async fn command_wakes_without_vad_and_receipt_follows_actual_append_and_snapshot() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        let frame = final_frame("first sentence");
        let ordinal = inject(&endpoint, &service, &frame);
        let receipt = endpoint
            .commit_through(ordinal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        let snapshot = observed_frame(&mut observations, |data| {
            matches!(
                data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await;
        assert!(endpoint
            .accept_local_context(&snapshot, Direction::Downstream)
            .unwrap());
        assert_eq!(receipt.through(), ordinal);
        assert!(receipt.effects_admitted());
        assert_eq!(user_text(&pair), ["first sentence"]);
        assert_eq!(
            endpoint
                .register_final(&frame, Direction::Upstream)
                .unwrap(),
            ordinal
        );
        assert!(endpoint
            .admit(
                &service.setup.lock().unwrap().clone().unwrap(),
                frame,
                Direction::Upstream
            )
            .unwrap());
        let again = endpoint
            .commit_through(ordinal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(again.sequence(), receipt.sequence());
        assert_eq!(user_text(&pair), ["first sentence"]);
        finish(worker).await;
    }
    #[tokio::test]
    async fn real_consumer_pause_does_not_issue_staging_or_receipt() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::ProcessorPauseUrgent {
                    processor: "LLMUserAggregator".into(),
                }),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::ProcessorPauseUrgent { .. })
        })
        .await;
        let ordinal = inject(&endpoint, &service, &final_frame("paused speech"));
        let mut command = endpoint.commit_through(ordinal).unwrap();
        assert!(matches!(
            command.receiver.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(user_text(&pair).is_empty());
        worker
            .sender()
            .send(
                Frame::new(FrameData::ProcessorResumeUrgent {
                    processor: "LLMUserAggregator".into(),
                }),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert_eq!(command.wait().await.unwrap().through(), ordinal);
        assert_eq!(user_text(&pair), ["paused speech"]);
        finish(worker).await;
    }
    #[tokio::test]
    async fn post_append_snapshot_admission_is_not_receipt_delivery() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .receipt_paused = true;
        let ordinal = inject(
            &endpoint,
            &service,
            &final_frame("truthful committed speech"),
        );
        let mut command = endpoint.commit_through(ordinal).unwrap();
        observed_frame(&mut observations, |data| {
            matches!(
                data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await;
        assert_eq!(user_text(&pair), ["truthful committed speech"]);
        assert!(matches!(
            command.receiver.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .receipt_paused = false;
        endpoint.realtime.changed.notify_one();
        assert_eq!(command.wait().await.unwrap().through(), ordinal);
        finish(worker).await;
    }
    #[tokio::test]
    async fn stop_between_actual_consumer_claim_and_reducer_preserves_terminal_speech() {
        let pair = make_pair(Default::default());
        let (endpoint, service, mut worker, mut observations) = start(&pair).await;
        let pause = HistoryTestPause::new();
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .consumer_pause = Some(pause.clone());
        let ordinal = inject(&endpoint, &service, &final_frame("claimed before Stop"));
        let command = endpoint.commit_through(ordinal).unwrap();
        tokio::time::timeout(Duration::from_secs(3), pause.entered.notified())
            .await
            .unwrap();
        assert!(user_text(&pair).is_empty());
        endpoint.close_releases().unwrap();
        assert!(command.wait().await.is_err());
        pause.resume.notify_one();
        worker
            .sender()
            .send(Frame::new(FrameData::Stop), Direction::Downstream)
            .await
            .unwrap();
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(user_text(&pair), ["claimed before Stop"]);
        while let Ok(frame) = observations.try_recv() {
            assert!(
                !matches!(
                    frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                ),
                "no feedback admitted after close"
            );
        }
    }
    #[tokio::test]
    async fn close_after_actual_append_before_snapshot_admission_is_orderly_and_truthful() {
        let pair = make_pair(Default::default());
        let (endpoint, service, mut worker, mut observations) = start(&pair).await;
        let pause = HistoryTestPause::new();
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .snapshot_pause = Some(pause.clone());
        let ordinal = inject(&endpoint, &service, &final_frame("append won before Stop"));
        let command = endpoint.commit_through(ordinal).unwrap();
        tokio::time::timeout(Duration::from_secs(3), pause.entered.notified())
            .await
            .unwrap();
        assert_eq!(user_text(&pair), ["append won before Stop"]);
        endpoint.close_releases().unwrap();
        assert!(command.wait().await.is_err());
        pause.resume.notify_one();
        worker
            .sender()
            .send(Frame::new(FrameData::Stop), Direction::Downstream)
            .await
            .unwrap();
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(user_text(&pair), ["append won before Stop"]);
        while let Ok(frame) = observations.try_recv() {
            assert!(
                !matches!(
                    frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                ),
                "no feedback admitted after close"
            );
        }
    }
    #[tokio::test]
    async fn real_snapshot_queue_full_retains_append_but_fails_the_receipt() {
        let pair = make_pair(Default::default());
        let (endpoint, service, mut worker, mut observations) = start(&pair).await;
        let pause = HistoryTestPause::new();
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .snapshot_pause = Some(pause.clone());
        let ordinal = inject(
            &endpoint,
            &service,
            &final_frame("retained after queue failure"),
        );
        let command = endpoint.commit_through(ordinal).unwrap();
        tokio::time::timeout(Duration::from_secs(3), pause.entered.notified())
            .await
            .unwrap();
        assert_eq!(user_text(&pair), ["retained after queue failure"]);
        worker
            .sender()
            .send(
                Frame::new(FrameData::ProcessorPauseUrgent {
                    processor: "history-test-service".into(),
                }),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::ProcessorPauseUrgent { .. })
        })
        .await;
        let user_route = endpoint
            .realtime
            .lock()
            .unwrap()
            .user_route
            .clone()
            .unwrap();
        // The system-lane pause witness leaves this normal lane empty. Fill
        // exactly its actual configured capacity; only the authentic snapshot
        // may perform the first failing enqueue (core fails on any Full).
        for _ in 0..pipecat_core::PipelineConfig::default().queue_capacity {
            user_route
                .try_push(Frame::new(FrameData::Base), Direction::Downstream)
                .unwrap();
        }
        pause.resume.notify_one();
        let failure = command.wait().await.unwrap_err();
        assert_eq!(
            failure.message,
            "CommittedButEffectsFailed: actual user history is retained"
        );
        assert!(matches!(
            worker.join().await,
            Err(pipecat_core::PipelineError::Backpressure(message))
                if message == "processor 2 input lane is full"
        ));
        assert_eq!(user_text(&pair), ["retained after queue failure"]);
        let state = endpoint.realtime.lock().unwrap();
        let history = state.history.as_ref().unwrap();
        assert_eq!(
            history.failure.as_deref(),
            Some("CommittedButEffectsFailed: actual user history is retained")
        );
        assert!(history.ledger.iter().all(|record| !record.admitted));
    }
    #[tokio::test]
    async fn checked_sequence_exhaustion_rolls_back_the_real_append() {
        let pair = make_pair(Default::default());
        let (endpoint, service, mut worker, mut observations) = start(&pair).await;
        let ordinal = inject(
            &endpoint,
            &service,
            &final_frame("must not partially append"),
        );
        worker
            .sender()
            .send(
                Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
                    returning: false,
                    origin: Some("accepted-sequence-boundary".into()),
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::Flush(_))
        })
        .await;
        {
            let mut state = endpoint.realtime.lock().unwrap();
            let history = state.history.as_mut().unwrap();
            assert_eq!(history.accepted_through, ordinal);
            assert_eq!(history.appended_through, 0);
            assert!(history.ledger.is_empty());
            // Exhaustion is a private lifetime-counter boundary injection;
            // registration, staging, acceptance and the commit remain real.
            history.sequence = u64::MAX;
        }
        assert!(endpoint
            .commit_through(ordinal)
            .unwrap()
            .wait()
            .await
            .is_err());
        assert!(matches!(
            worker.join().await,
            Err(pipecat_core::PipelineError::Execution(message))
                if message == "user commit sequence exhausted"
        ));
        assert!(user_text(&pair).is_empty());
        let state = endpoint.realtime.lock().unwrap();
        let history = state.history.as_ref().unwrap();
        assert_eq!(history.sequence, u64::MAX);
        assert_eq!(history.appended_through, 0);
        assert!(history.ledger.is_empty());
    }
    async fn automatic_flush_requires_original_consumer_acceptance(timer: bool) {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        let pause = HistoryTestPause::new();
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .staged_pause = Some(pause.clone());
        let ordinal = inject(&endpoint, &service, &final_frame("staged actual speech"));
        tokio::time::timeout(Duration::from_secs(3), pause.entered.notified())
            .await
            .unwrap();
        let mut command = endpoint.commit_through(ordinal).unwrap();
        {
            let state = endpoint.realtime.lock().unwrap();
            let history = state.history.as_ref().unwrap();
            assert_eq!(history.accepted_through, 0);
            assert!(history.registrations[&ordinal].staged);
            assert!(history.registrations[&ordinal].processing);
        }
        assert!(user_text(&pair).is_empty());
        let trigger = tokio::time::Instant::now();
        let setup = service.setup.lock().unwrap().clone().unwrap();
        assert!(endpoint
            .admit(
                &setup,
                Frame::new(if timer {
                    FrameData::LlmResponseStart
                } else {
                    FrameData::LlmResponseEnd
                }),
                Direction::Downstream
            )
            .unwrap());
        let snapshot = observed_frame(&mut observations, |data| {
            matches!(
                data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await;
        assert!(endpoint
            .accept_local_context(&snapshot, Direction::Downstream)
            .unwrap());
        if timer {
            assert!(tokio::time::Instant::now().duration_since(trigger) >= Duration::from_secs(1));
        }
        assert_eq!(user_text(&pair), ["staged actual speech"]);
        assert!(matches!(
            command.receiver.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        {
            let state = endpoint.realtime.lock().unwrap();
            let history = state.history.as_ref().unwrap();
            assert_eq!(history.accepted_through, 0);
            assert_eq!(history.appended_through, ordinal);
            assert_eq!(history.ledger.len(), 1);
            assert!(history.ledger[0].admitted);
        }
        pause.resume.notify_one();
        let receipt = command.wait().await.unwrap();
        assert_eq!(receipt.through(), ordinal);
        assert_eq!(receipt.sequence(), 1);
        assert_eq!(user_text(&pair), ["staged actual speech"]);
        finish(worker).await;
    }
    #[tokio::test(start_paused = true)]
    async fn real_assistant_response_end_flush_waits_for_the_original_consumer_guard() {
        automatic_flush_requires_original_consumer_acceptance(false).await;
    }
    #[tokio::test(start_paused = true)]
    async fn real_handoff_deadline_flush_waits_for_the_original_consumer_guard() {
        automatic_flush_requires_original_consumer_acceptance(true).await;
    }
    #[tokio::test]
    async fn dropped_waiter_does_not_erase_the_actual_committed_message() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .receipt_paused = true;
        let ordinal = inject(
            &endpoint,
            &service,
            &final_frame("committed independently of waiter"),
        );
        let command = endpoint.commit_through(ordinal).unwrap();
        observed_frame(&mut observations, |data| {
            matches!(
                data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await;
        drop(command);
        assert_eq!(user_text(&pair), ["committed independently of waiter"]);
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .receipt_paused = false;
        let receipt = endpoint
            .commit_through(ordinal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(receipt.through(), ordinal);
        assert_eq!(receipt.sequence(), 1);
        finish(worker).await;
    }
    #[tokio::test]
    async fn closing_keeps_accepted_trailing_text_for_actual_terminal_append() {
        let pair = make_pair(Default::default());
        let (endpoint, service, mut worker, mut observations) = start(&pair).await;
        inject(&endpoint, &service, &final_frame("terminal speech"));
        // A real downstream normal fence proves the earlier final handler ran.
        worker
            .sender()
            .send(
                Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
                    returning: false,
                    origin: Some("test-consumer-barrier".into()),
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::Flush(_))
        })
        .await;
        assert!(user_text(&pair).is_empty());
        endpoint.close_releases().unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::Stop), Direction::Downstream)
            .await
            .unwrap();
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(user_text(&pair), ["terminal speech"]);
        assert!(endpoint.realtime.lock().unwrap().user_route.is_none());
        assert!(endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_ref()
            .unwrap()
            .service_setup
            .is_none());
    }
    #[tokio::test]
    async fn exact_end_marker_quiesces_controls_but_commit_service_stays_live() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        endpoint.begin_draining().unwrap();
        let marker = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
            returning: false,
            origin: Some("gemini_history_end_input".into()),
        }));
        assert!(!marker.interruptible);
        endpoint
            .register_end_marker(&marker, Direction::Downstream)
            .unwrap();
        assert!(endpoint.admit_end_marker(&marker).unwrap());
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::Flush(_))
        })
        .await;
        assert!(pair.user().history_controls_closed());
        assert!(endpoint
            .validate_end_marker(&marker, Direction::Downstream)
            .is_err());
        let ordinal = inject(
            &endpoint,
            &service,
            &final_frame("provider suffix after finish"),
        );
        assert_eq!(
            endpoint
                .commit_through(ordinal)
                .unwrap()
                .wait()
                .await
                .unwrap()
                .through(),
            ordinal
        );
        assert_eq!(user_text(&pair), ["provider suffix after finish"]);
        finish(worker).await;
    }
    #[tokio::test(start_paused = true)]
    async fn actual_timer_control_extracted_before_end_marker_can_arrive_after_it() {
        let pair = make_pair(UserAggregatorConfig {
            turn: crate::UserTurnConfig {
                enable_interruptions: false,
                speech_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        });
        let (endpoint, service, worker, mut observations) = start(&pair).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::VadUserStartedSpeaking(
                    pipecat_core::VadSpeakingFrame {
                        duration_secs: 0.0,
                        timestamp: 0.0,
                    },
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::UserStartedSpeaking)
        })
        .await;
        let pause = HistoryTestPause::new();
        endpoint
            .realtime
            .lock()
            .unwrap()
            .history
            .as_mut()
            .unwrap()
            .timer_pause = Some(pause.clone());
        worker
            .sender()
            .send(
                Frame::new(FrameData::VadUserStoppedSpeaking(
                    pipecat_core::VadSpeakingFrame {
                        duration_secs: 0.1,
                        timestamp: 0.0,
                    },
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::VadUserStoppedSpeaking(_))
        })
        .await;
        tokio::time::advance(Duration::from_millis(101)).await;
        tokio::time::timeout(Duration::from_secs(3), pause.entered.notified())
            .await
            .unwrap();
        endpoint.begin_draining().unwrap();
        let marker = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
            returning: false,
            origin: Some("gemini_history_end_input".into()),
        }));
        endpoint
            .register_end_marker(&marker, Direction::Downstream)
            .unwrap();
        assert!(endpoint.admit_end_marker(&marker).unwrap());
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::Flush(_))
        })
        .await;
        assert!(pair.user().history_controls_closed());
        pause.resume.notify_one();
        // This is an actual previously extracted controller effect. The service
        // must apply its own post-marker provider-input phase when it arrives.
        observed_frame(&mut observations, |data| {
            matches!(data, FrameData::UserStoppedSpeaking)
        })
        .await;
        let ordinal = inject(&endpoint, &service, &final_frame("history remains live"));
        assert_eq!(
            endpoint
                .commit_through(ordinal)
                .unwrap()
                .wait()
                .await
                .unwrap()
                .through(),
            ordinal
        );
        finish(worker).await;
    }
    #[tokio::test]
    async fn real_wake_strategy_reset_fails_instead_of_discarding_registered_speech() {
        let strategy = crate::UserTurnStartStrategy::wake_phrase(
            vec!["wake now".into()],
            Duration::from_secs(2),
            false,
        )
        .unwrap();
        let pair = make_pair(UserAggregatorConfig {
            turn: crate::UserTurnConfig {
                start_strategies: Some(
                    crate::UserTurnStartStrategies::new(vec![strategy]).unwrap(),
                ),
                ..Default::default()
            },
            ..Default::default()
        });
        let (endpoint, service, mut worker, _) = start(&pair).await;
        let ordinal = inject(&endpoint, &service, &final_frame("does not match"));
        let receipt = endpoint.commit_through(ordinal).unwrap().wait().await;
        assert!(receipt.is_err());
        assert!(matches!(
            worker.join().await,
            Err(pipecat_core::PipelineError::Execution(message))
                if message == "strategy reset would discard registered user history"
        ));
        assert!(worker
            .errors()
            .iter()
            .any(|failure| format!("{failure:?}").contains("strategy reset")));
        assert!(user_text(&pair).is_empty());
    }
    #[tokio::test]
    async fn seal_mutations_fresh_token_wire_copy_and_lineage_do_not_certify_history() {
        let pair = make_pair(Default::default());
        let (endpoint, service, worker, _) = start(&pair).await;
        let frame = final_frame("sealed speech");
        let ordinal = endpoint
            .register_final(&frame, Direction::Upstream)
            .unwrap();
        let mut mutated = frame.clone();
        mutated.interruptible = true;
        assert!(endpoint
            .register_final(&mutated, Direction::Upstream)
            .is_err());
        let mut changed = frame.clone();
        changed.data = FrameData::LlmRun;
        let user_ctx = endpoint
            .realtime
            .lock()
            .unwrap()
            .user_route
            .clone()
            .unwrap();
        assert!(endpoint
            .realtime
            .history_processing(&changed, Direction::Upstream, &user_ctx)
            .is_err());
        let mut fresh = frame.duplicate();
        fresh.id = frame.id;
        assert!(endpoint
            .realtime
            .history_processing(&fresh, Direction::Upstream, &user_ctx)
            .is_err());
        let wire: Frame = serde_json::from_value(serde_json::to_value(&frame).unwrap()).unwrap();
        assert!(endpoint
            .realtime
            .history_processing(&wire, Direction::Upstream, &user_ctx)
            .is_err());
        assert!(endpoint
            .admit(
                &service.setup.lock().unwrap().clone().unwrap(),
                frame,
                Direction::Upstream
            )
            .unwrap());
        let receipt = endpoint
            .commit_through(ordinal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        pair.context().replace(Context::default()).unwrap();
        assert!(endpoint.validate_receipt(&receipt).is_err());
        finish(worker).await;
    }
    #[test]
    fn selected_assistant_rejects_upstream_transcripts_before_consuming_them() {
        let pair = make_pair(Default::default());
        pair.reserve_realtime_user_history().unwrap();
        for data in [
            FrameData::Transcription(TranscriptionFrame::default()),
            FrameData::InterimTranscription(TranscriptionFrame::default()),
            FrameData::EagerTranscription(TranscriptionFrame::default()),
            FrameData::Translation(TranscriptionFrame::default()),
        ] {
            assert!(pair
                .assistant()
                .process_at(Frame::new(data), Direction::Upstream, Duration::ZERO)
                .is_err());
        }
        assert!(pair
            .assistant()
            .process_at(
                Frame::new(FrameData::InterimTranscription(
                    TranscriptionFrame::default()
                )),
                Direction::Downstream,
                Duration::ZERO,
            )
            .is_ok());
        assert!(user_text(&pair).is_empty());
    }
    #[test]
    fn reservation_is_once_unused_enabled_and_bounds_precede_seal_cloning() {
        let pair = make_pair(Default::default());
        assert!(pair.reserve_realtime_user_history().is_ok());
        assert!(pair.reserve_realtime_user_history().is_err());
        let auto = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        assert!(auto.reserve_realtime_user_history().is_err());
        let used = make_pair(Default::default());
        used.user()
            .process_at(
                Frame::new(FrameData::Base),
                Direction::Downstream,
                Duration::ZERO,
            )
            .unwrap();
        assert!(used.reserve_realtime_user_history().is_err());
        assert!(Seal::new(&final_frame(&"x".repeat(65537)), Direction::Upstream).is_err());
        let mut deep = serde_json::Value::Null;
        for _ in 0..18 {
            deep = serde_json::json!([deep]);
        }
        let mut frame = final_frame("bounded");
        frame.metadata.insert("deep".into(), deep);
        assert!(Seal::new(&frame, Direction::Upstream).is_err());
    }
}
