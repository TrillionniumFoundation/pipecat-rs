//! One receive owner for the selected, bounded history route.
//!
//! This task owns the provider session and its real worker join. Receipts are
//! consumer-issued; source coverage alone never authorizes a nonempty release.

use super::{
    finalizer::FinalizedSentence, finalizer::SourceFinalizer, HistoryControl, SelectedHistory,
};
use crate::{
    client::ReceivedEvent,
    processor::{attach_usage_details, processor_error},
    protocol::InputPacket,
    Event, GeminiError, GeminiFrameMapper, GeminiSender, GeminiSession, SessionMode,
};
use futures_util::future::BoxFuture;
use pipecat_core::{
    Direction, Frame, FrameData, ProcessorContext, ProcessorError, TranscriptionFrame,
};
use pipecat_turns::CommittedUserPrefix;
use std::{collections::BTreeMap, collections::VecDeque, sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

const MAX_IDENTITIES: usize = 256;

fn error(message: impl Into<String>) -> ProcessorError {
    ProcessorError::fatal(message)
}

fn provider_error(source: GeminiError) -> ProcessorError {
    let mut result = processor_error(source);
    result.fatal = true;
    result
}

fn add(left: usize, right: usize) -> Result<usize, ProcessorError> {
    left.checked_add(right)
        .ok_or_else(|| error("history receive byte counter exhausted"))
}

fn deadline(now: Instant, duration: Duration) -> Result<Instant, ProcessorError> {
    now.checked_add(duration)
        .ok_or_else(|| error("history receive deadline exhausted"))
}

struct QueuedFinal {
    frame: Frame,
    ordinal: u64,
    bytes: usize,
}

struct Coverage {
    source_through: u64,
    ordinal_through: u64,
}

struct HeldEvent {
    received: ReceivedEvent,
    source_through: u64,
    own_bytes: usize,
    deadline: Instant,
}

struct PacketCost {
    references: usize,
    bytes: usize,
}

struct PendingCommit {
    through: u64,
    deadline: Instant,
    wait: BoxFuture<'static, Result<CommittedUserPrefix, ProcessorError>>,
}

enum SourceDependency {
    NoHeldOutput,
    EndOfInput,
    Unfinalized,
    Finalized(u64),
}

#[derive(Debug, PartialEq, Eq)]
struct PrefixPlan {
    admit_through: u64,
    commit_through: Option<u64>,
}

fn prefix_plan(
    dependency: SourceDependency,
    finalized: u64,
    registered: u64,
    committed: u64,
) -> PrefixPlan {
    let target = match dependency {
        SourceDependency::NoHeldOutput | SourceDependency::Unfinalized => {
            // Dispatch punctuation promptly, but do not manufacture a user
            // message boundary without a complete output or End dependency.
            return PrefixPlan {
                admit_through: finalized,
                commit_through: None,
            };
        }
        SourceDependency::Finalized(required) => required.max(registered),
        SourceDependency::EndOfInput => finalized.max(registered),
    };
    PrefixPlan {
        admit_through: target,
        commit_through: (target > committed).then_some(target),
    }
}

struct ReceiveState {
    finalizer: SourceFinalizer,
    mapper: GeminiFrameMapper,
    user_id: String,
    language: Option<String>,
    generation: u64,
    last_packet: u64,
    received_source: u64,
    covered_source: u64,
    committed_source: u64,
    final_ordinal: u64,
    registered_ordinal: u64,
    committed_ordinal: u64,
    receipt: Option<CommittedUserPrefix>,
    coverage: VecDeque<Coverage>,
    finals: VecDeque<QueuedFinal>,
    final_bytes: usize,
    inflight_count: usize,
    inflight_bytes: usize,
    pending: Option<PendingCommit>,
    held: VecDeque<HeldEvent>,
    held_bytes: usize,
    packet_costs: BTreeMap<u64, PacketCost>,
}

impl ReceiveState {
    fn new(
        owner: &SelectedHistory,
        user_id: String,
        generation: u64,
    ) -> Result<Self, ProcessorError> {
        Ok(Self {
            finalizer: SourceFinalizer::new(
                owner.options.max_fragment_bytes,
                owner.options.max_suffix_bytes,
            )
            .map_err(provider_error)?,
            mapper: GeminiFrameMapper::new(SessionMode::Conversation, user_id.clone()),
            user_id,
            language: None,
            generation,
            last_packet: 0,
            received_source: 0,
            covered_source: 0,
            committed_source: 0,
            final_ordinal: 0,
            registered_ordinal: 0,
            committed_ordinal: 0,
            receipt: None,
            coverage: VecDeque::new(),
            finals: VecDeque::new(),
            final_bytes: 0,
            inflight_count: 0,
            inflight_bytes: 0,
            pending: None,
            held: VecDeque::new(),
            held_bytes: 0,
            packet_costs: BTreeMap::new(),
        })
    }

    fn packet(
        &mut self,
        owner: &SelectedHistory,
        packet: &InputPacket,
        now: Instant,
    ) -> Result<(), ProcessorError> {
        if packet.sequence == 0 {
            return Err(error("history packet sequence starts at one"));
        }
        if packet.sequence <= self.last_packet {
            // Deferred thinking completions can retain an older packet. Its
            // original dependency survives, but its actual text is not replayed.
            if packet.received_input_watermark > self.received_source {
                return Err(error("older history packet contains unseen source"));
            }
            return Ok(());
        }
        self.last_packet = packet.sequence;
        if let Some(transcript) = packet.actual_input.as_ref().filter(|t| !t.text.is_empty()) {
            let finals = self
                .finalizer
                .ingest(packet.received_input_watermark, transcript, now)
                .map_err(provider_error)?;
            self.received_source = packet.received_input_watermark;
            self.language = transcript.language_code.clone();
            self.stage(owner, finals)?;
        } else if packet.received_input_watermark != self.received_source {
            return Err(error("history packet skipped an actual input contribution"));
        }
        self.note_coverage(self.finalizer.covered_watermark())?;
        Ok(())
    }

    fn expire(&mut self, owner: &SelectedHistory, now: Instant) -> Result<(), ProcessorError> {
        let finals = self.finalizer.expire(now).map_err(provider_error)?;
        self.stage(owner, finals)?;
        self.note_coverage(self.finalizer.covered_watermark())
    }

    fn stage(
        &mut self,
        owner: &SelectedHistory,
        finals: Vec<FinalizedSentence>,
    ) -> Result<(), ProcessorError> {
        for sentence in finals {
            if add(self.finals.len(), self.inflight_count)? >= MAX_IDENTITIES {
                return Err(error("history receive final identities exceed 256"));
            }
            let language = sentence.result.as_ref().map_or_else(
                || self.language.clone(),
                |source| source.language_code.clone(),
            );
            // Materializing frame JSON intentionally counts every owned copy.
            // The source finalizer shares provenance, but frame payloads do not.
            let result = sentence
                .result
                .as_deref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|e| error(format!("history provenance serialization failed: {e}")))?;
            let timestamp = chrono::Utc::now().to_rfc3339();
            let mut bytes = add(128, sentence.text.len())?;
            bytes = add(bytes, self.user_id.len())?;
            bytes = add(bytes, timestamp.len())?;
            bytes = add(bytes, language.as_ref().map_or(0, String::len))?;
            if let Some(result) = &result {
                bytes = add(
                    bytes,
                    serde_json::to_vec(result)
                        .map_err(|e| error(format!("history provenance sizing failed: {e}")))?
                        .len(),
                )?;
            }
            let retained = add(self.final_bytes, bytes)?;
            if retained > owner.options.max_queued_final_bytes {
                return Err(error("history receive finalized payload bound exceeded"));
            }
            let ordinal = self
                .final_ordinal
                .checked_add(1)
                .ok_or_else(|| error("history receive final ordinal exhausted"))?;
            let mut frame = Frame::new(FrameData::Transcription(TranscriptionFrame {
                text: sentence.text,
                user_id: self.user_id.clone(),
                timestamp,
                language,
                result,
                finalized: true,
            }));
            // Exact setup-route registration is the sole authority for this
            // ordinary noninterruptible final; no generation metadata is used.
            frame.interruptible = false;
            self.finals.push_back(QueuedFinal {
                frame,
                ordinal,
                bytes,
            });
            self.final_bytes = retained;
            self.final_ordinal = ordinal;
            self.note_coverage(sentence.source_through)?;
        }
        Ok(())
    }

    fn note_coverage(&mut self, source_through: u64) -> Result<(), ProcessorError> {
        if source_through < self.covered_source || source_through > self.received_source {
            return Err(error(
                "history source coverage is not a received monotonic prefix",
            ));
        }
        if source_through == self.covered_source {
            return Ok(());
        }
        self.covered_source = source_through;
        if let Some(last) = self
            .coverage
            .back_mut()
            .filter(|last| last.ordinal_through == self.final_ordinal)
        {
            last.source_through = source_through;
        } else {
            if self.coverage.len() > MAX_IDENTITIES {
                return Err(error("history source coverage records exceed bound"));
            }
            self.coverage.push_back(Coverage {
                source_through,
                ordinal_through: self.final_ordinal,
            });
        }
        self.advance_coverage();
        Ok(())
    }

    fn advance_coverage(&mut self) {
        while self
            .coverage
            .front()
            .is_some_and(|entry| entry.ordinal_through <= self.committed_ordinal)
        {
            if let Some(entry) = self.coverage.pop_front() {
                self.committed_source = entry.source_through;
            }
        }
    }

    fn needed_ordinal(&self, source: u64) -> Option<u64> {
        if source <= self.committed_source {
            return Some(self.committed_ordinal);
        }
        self.coverage
            .iter()
            .find(|entry| entry.source_through >= source)
            .map(|entry| entry.ordinal_through)
    }

    fn hold(
        &mut self,
        owner: &SelectedHistory,
        received: ReceivedEvent,
        now: Instant,
    ) -> Result<(), ProcessorError> {
        let source_through = received
            .packet
            .as_ref()
            .map_or(0, |packet| packet.received_input_watermark);
        if source_through > self.received_source {
            return Err(error("history output depends on unobserved source"));
        }
        let own_bytes = event_bytes(&received)?;
        let held = HeldEvent {
            received,
            source_through,
            own_bytes,
            deadline: deadline(now, owner.options.consumer_timeout)?,
        };
        self.hold_existing(owner, held)
    }

    fn hold_existing(
        &mut self,
        owner: &SelectedHistory,
        held: HeldEvent,
    ) -> Result<(), ProcessorError> {
        if self.held.len() >= owner.options.max_held_events {
            return Err(error("history held event count exceeded"));
        }
        let mut bytes = add(self.held_bytes, held.own_bytes)?;
        if let Some(packet) = &held.received.packet {
            if !self.packet_costs.contains_key(&packet.sequence) {
                bytes = add(bytes, packet_bytes(packet)?)?;
            }
        }
        if bytes > owner.options.max_held_bytes {
            return Err(error("history held event payload bound exceeded"));
        }
        if let Some(packet) = &held.received.packet {
            let packet_size = packet_bytes(packet)?;
            let cost = self
                .packet_costs
                .entry(packet.sequence)
                .or_insert(PacketCost {
                    references: 0,
                    bytes: packet_size,
                });
            cost.references = cost
                .references
                .checked_add(1)
                .ok_or_else(|| error("history held packet reference counter exhausted"))?;
        }
        self.held_bytes = bytes;
        self.held.push_back(held);
        Ok(())
    }

    fn pop_held(&mut self) -> Result<Option<HeldEvent>, ProcessorError> {
        let Some(held) = self.held.pop_front() else {
            return Ok(None);
        };
        let mut removed = held.own_bytes;
        if let Some(packet) = &held.received.packet {
            let cost = self
                .packet_costs
                .get_mut(&packet.sequence)
                .ok_or_else(|| error("history held packet accounting missing"))?;
            cost.references = cost
                .references
                .checked_sub(1)
                .ok_or_else(|| error("history held packet reference counter underflow"))?;
            if cost.references == 0 {
                removed = add(removed, cost.bytes)?;
                self.packet_costs.remove(&packet.sequence);
            }
        }
        self.held_bytes = self
            .held_bytes
            .checked_sub(removed)
            .ok_or_else(|| error("history held byte counter underflow"))?;
        Ok(Some(held))
    }

    fn generation(&mut self, owner: &SelectedHistory, current: u64) -> Result<(), ProcessorError> {
        if self.generation == current {
            return Ok(());
        }
        self.generation = current;
        self.mapper.reset();
        let held = std::mem::take(&mut self.held);
        self.held_bytes = 0;
        self.packet_costs.clear();
        for event in held {
            if event.received.event.generation == current {
                self.hold_existing(owner, event)?;
            }
        }
        // User fragments, real registrations and an in-flight receipt survive
        // assistant generation retirement. They belong to this session.
        Ok(())
    }

    fn request_commit(
        &mut self,
        owner: &SelectedHistory,
        output: &ProcessorContext,
        now: Instant,
        provider_eof: bool,
    ) -> Result<(), ProcessorError> {
        if self.pending.is_some() {
            return Ok(());
        }
        // A ready production dependency selects its finalized prefix. Without
        // production, only joined provider EOF with no source suffix is a final
        // barrier; ordinary Open input must not force per-sentence user messages.
        let dependency = if let Some(event) = self.held.front() {
            match self.needed_ordinal(event.source_through) {
                Some(through) => SourceDependency::Finalized(through),
                None => SourceDependency::Unfinalized,
            }
        } else if provider_eof && self.finalizer.deadline().is_none() {
            SourceDependency::EndOfInput
        } else {
            SourceDependency::NoHeldOutput
        };
        let plan = prefix_plan(
            dependency,
            self.final_ordinal,
            self.registered_ordinal,
            self.committed_ordinal,
        );
        while self
            .finals
            .front()
            .is_some_and(|finalized| finalized.ordinal <= plan.admit_through)
        {
            let finalized = self
                .finals
                .pop_front()
                .ok_or_else(|| error("history final queue changed during dispatch"))?;
            let ordinal = owner
                .endpoint
                .register_final(&finalized.frame, Direction::Upstream)?;
            if ordinal != finalized.ordinal
                || ordinal
                    != self
                        .registered_ordinal
                        .checked_add(1)
                        .ok_or_else(|| error("history registered ordinal exhausted"))?
            {
                return Err(error(
                    "history endpoint returned an unexpected real final ordinal",
                ));
            }
            self.registered_ordinal = ordinal;
            if !owner
                .endpoint
                .admit(output, finalized.frame, Direction::Upstream)?
            {
                return Err(error("history final admission closed"));
            }
            self.inflight_count = add(self.inflight_count, 1)?;
            self.inflight_bytes = add(self.inflight_bytes, finalized.bytes)?;
        }
        let Some(target) = plan.commit_through else {
            return Ok(());
        };
        // Punctuation finals may already have been dispatched while source
        // coverage was incomplete. Request their actual registered prefix even
        // when the final queue is now empty (including whitespace-only expiry).
        if self.registered_ordinal != target {
            return Err(error(
                "history commit target is not an actually dispatched prefix",
            ));
        }
        let request = owner.endpoint.commit_through(target)?;
        self.pending = Some(PendingCommit {
            through: target,
            deadline: deadline(now, owner.options.consumer_timeout)?,
            wait: Box::pin(request.wait()),
        });
        Ok(())
    }

    fn accept_receipt(
        &mut self,
        owner: &SelectedHistory,
        receipt: CommittedUserPrefix,
    ) -> Result<(), ProcessorError> {
        let pending = self
            .pending
            .take()
            .ok_or_else(|| error("history received a receipt without a pending command"))?;
        owner.endpoint.validate_receipt(&receipt)?;
        if !receipt.effects_admitted()
            || receipt.through() != pending.through
            || receipt.through() != self.registered_ordinal
            || receipt.through() <= self.committed_ordinal
            || self
                .receipt
                .as_ref()
                .is_some_and(|previous| receipt.sequence() <= previous.sequence())
        {
            return Err(error(
                "history receipt does not commit the selected actual prefix",
            ));
        }
        self.committed_ordinal = receipt.through();
        self.receipt = Some(receipt);
        self.final_bytes = self
            .final_bytes
            .checked_sub(self.inflight_bytes)
            .ok_or_else(|| error("history committed final byte counter underflow"))?;
        self.inflight_count = 0;
        self.inflight_bytes = 0;
        self.advance_coverage();
        Ok(())
    }

    async fn release(
        &mut self,
        owner: &SelectedHistory,
        sender: &GeminiSender,
        output: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        while self
            .held
            .front()
            .is_some_and(|event| event.source_through <= self.committed_source)
        {
            let event = self
                .pop_held()?
                .ok_or_else(|| error("history held queue changed during release"))?;
            if event.source_through != 0 {
                if let Some(receipt) = &self.receipt {
                    owner.endpoint.validate_receipt(receipt)?;
                }
                // With no receipt, this source prefix consists entirely of
                // recorded no-content results; committed_ordinal is still zero.
            }
            let generation = event.received.event.generation;
            if let Some(result) = sender
                .while_current(generation, async {
                    emit(owner, output, &mut self.mapper, event.received)
                })
                .await
            {
                result?;
            }
        }
        Ok(())
    }

    fn consumer_deadline(&self) -> Option<Instant> {
        [
            self.held.front().map(|event| event.deadline),
            self.pending.as_ref().map(|pending| pending.deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn is_drained(&self) -> bool {
        self.finalizer.deadline().is_none()
            && self.finals.is_empty()
            && self.pending.is_none()
            && self.held.is_empty()
            && self.inflight_count == 0
            && self.final_ordinal == self.committed_ordinal
            && self.received_source == self.committed_source
    }
}

fn packet_bytes(packet: &InputPacket) -> Result<usize, ProcessorError> {
    let mut bytes = 64;
    if let Some(source) = &packet.actual_input {
        bytes = add(bytes, source.text.len())?;
        bytes = add(bytes, source.language_code.as_ref().map_or(0, String::len))?;
    }
    Ok(bytes)
}

fn event_bytes(received: &ReceivedEvent) -> Result<usize, ProcessorError> {
    let mut bytes = add(
        128,
        received
            .event
            .origin
            .context_id
            .as_ref()
            .map_or(0, String::len),
    )?;
    bytes = add(
        bytes,
        match &received.event.event {
            Event::Text { text, .. } => text.len(),
            Event::Audio { pcm, .. } => pcm.len(),
            Event::InputTranscription { transcript, .. }
            | Event::OutputTranscription(transcript) => add(
                transcript.text.len(),
                transcript.language_code.as_ref().map_or(0, String::len),
            )?,
            Event::GoAway { time_left } => time_left.len(),
            Event::ToolCalls(_)
            | Event::NonBlockingToolCalls(_)
            | Event::ToolCallsCancelled(_)
            | Event::NonBlockingToolCallsCancelled(_) => {
                return Err(error("paired history excludes provider tool events"));
            }
            _ => 0,
        },
    )?;
    Ok(bytes)
}

fn emit(
    owner: &SelectedHistory,
    output: &ProcessorContext,
    mapper: &mut GeminiFrameMapper,
    received: ReceivedEvent,
) -> Result<(), ProcessorError> {
    for mut frame in mapper.frames(&received.event) {
        if let Some(details) = &received.usage_details {
            attach_usage_details(&mut frame, details);
        }
        if !owner.endpoint.admit(output, frame, Direction::Downstream)? {
            return Err(error("history production release closed"));
        }
    }
    Ok(())
}

async fn wait_receipt(
    pending: &mut Option<PendingCommit>,
) -> Result<CommittedUserPrefix, ProcessorError> {
    match pending {
        Some(pending) => pending.wait.as_mut().await,
        None => std::future::pending().await,
    }
}

async fn wait_finish(
    finish: &mut Option<BoxFuture<'static, Result<(), GeminiError>>>,
) -> Result<(), GeminiError> {
    match finish {
        Some(finish) => finish.as_mut().await,
        None => std::future::pending().await,
    }
}

enum Wake {
    Control(Result<(), watch::error::RecvError>),
    Clock,
    Receipt(Result<CommittedUserPrefix, ProcessorError>),
    Finish(Result<(), GeminiError>),
    Event(Option<Result<ReceivedEvent, GeminiError>>),
}

async fn receive(
    owner: &SelectedHistory,
    session: &mut GeminiSession,
    output: &ProcessorContext,
    state: &mut ReceiveState,
    emit_metrics: bool,
) -> Result<(), ProcessorError> {
    let sender = session.sender();
    let mut control = owner.control.subscribe();
    let mut finish: Option<BoxFuture<'static, Result<(), GeminiError>>> = None;
    let mut finish_admitted = false;
    let mut drain_deadline = None;
    let mut eof = false;
    loop {
        let now = Instant::now();
        let requested = *control.borrow_and_update();
        match requested {
            HistoryControl::Cancel => return Ok(()),
            HistoryControl::Drain if drain_deadline.is_none() => {
                drain_deadline = Some(deadline(now, owner.options.drain_timeout)?);
                let sender = sender.clone();
                finish = Some(Box::pin(async move { sender.finish().await }));
            }
            _ => {}
        }
        state.generation(owner, sender.generation())?;
        if drain_deadline.is_some_and(|end| now >= end) {
            return Err(provider_error(GeminiError::Timeout("history End drain")));
        }
        if state.consumer_deadline().is_some_and(|end| now >= end) {
            return Err(provider_error(GeminiError::Timeout(
                "history consumer or source prefix",
            )));
        }
        if state.finalizer.deadline().is_some_and(|end| now >= end) {
            state.expire(owner, now)?;
        }
        let admission = async {
            state.release(owner, &sender, output).await?;
            state.request_commit(owner, output, now, eof)
        }
        .await;
        // A gate cancellation closes endpoint admission concurrently with this
        // short work. Its intentional revocation is not a new fatal failure.
        if *control.borrow() == HistoryControl::Cancel {
            return Ok(());
        }
        admission?;
        if eof && state.is_drained() {
            return if requested == HistoryControl::Drain && finish_admitted {
                Ok(())
            } else {
                Err(provider_error(GeminiError::Closed))
            };
        }
        let next_deadline = [
            state.finalizer.deadline(),
            state.consumer_deadline(),
            drain_deadline,
        ]
        .into_iter()
        .flatten()
        .min();
        let wake = tokio::select! { biased;
            changed = control.changed() => Wake::Control(changed),
            _ = tokio::time::sleep_until(next_deadline.unwrap_or(now)), if next_deadline.is_some() => Wake::Clock,
            receipt = wait_receipt(&mut state.pending), if state.pending.is_some() => Wake::Receipt(receipt),
            result = wait_finish(&mut finish), if finish.is_some() => Wake::Finish(result),
            event = session.next_event_with_details(), if !eof => Wake::Event(event),
        };
        if *control.borrow() == HistoryControl::Cancel
            && !matches!(&wake, Wake::Event(Some(Err(_))))
        {
            return Ok(());
        }
        match wake {
            Wake::Control(changed) => changed.map_err(|_| error("history control owner closed"))?,
            Wake::Clock => {}
            Wake::Receipt(receipt) => {
                let accepted = receipt.and_then(|receipt| state.accept_receipt(owner, receipt));
                if *control.borrow() == HistoryControl::Cancel {
                    return Ok(());
                }
                accepted?;
            }
            Wake::Finish(result) => {
                finish = None;
                result.map_err(provider_error)?;
                finish_admitted = true;
            }
            Wake::Event(None) => {
                // next_event_with_details has awaited the real provider worker.
                // Keep polling source timeout/receipts after this joined EOF.
                // An unsolicited EOF during Open must not promote a raw suffix
                // to history or release dependent output after disconnection.
                if *control.borrow() != HistoryControl::Drain || !finish_admitted {
                    return Err(provider_error(GeminiError::Closed));
                }
                eof = true;
            }
            Wake::Event(Some(Err(source))) => return Err(provider_error(source)),
            Wake::Event(Some(Ok(received))) => {
                if let Some(packet) = &received.packet {
                    state.packet(owner, packet, Instant::now())?;
                } else {
                    return Err(error("selected provider event lacks packet provenance"));
                }
                match &received.event.event {
                    Event::ToolCalls(_)
                    | Event::NonBlockingToolCalls(_)
                    | Event::ToolCallsCancelled(_)
                    | Event::NonBlockingToolCallsCancelled(_) => {
                        return Err(error("paired history excludes provider tool events"));
                    }
                    Event::GoAway { .. } => {
                        return Err(provider_error(GeminiError::Unsupported(
                            "paired history does not resume a disconnected session",
                        )))
                    }
                    Event::Interrupted => {
                        state.mapper.reset();
                        let interrupted = output.broadcast_interruption().await;
                        if *control.borrow() == HistoryControl::Cancel {
                            return Ok(());
                        }
                        interrupted?;
                    }
                    Event::InputTranscription { is_final: true, .. } => {
                        // The private packet contribution above is authoritative.
                        // Public actual fragments are never mistaken for finals.
                    }
                    Event::Usage(_) if !emit_metrics => {}
                    _ if received.event.event.is_output() => {
                        state.hold(owner, received, Instant::now())?;
                    }
                    _ => {
                        let emitted = emit(owner, output, &mut state.mapper, received);
                        if *control.borrow() == HistoryControl::Cancel {
                            return Ok(());
                        }
                        emitted?;
                    }
                }
            }
        }
    }
}

pub(super) async fn run(
    owner: Arc<SelectedHistory>,
    mut session: GeminiSession,
    output: ProcessorContext,
    user_id: String,
    emit_metrics: bool,
) -> Result<(), ProcessorError> {
    let outcome = match ReceiveState::new(&owner, user_id, session.sender().generation()) {
        Ok(mut state) => receive(&owner, &mut session, &output, &mut state, emit_metrics).await,
        Err(failure) => Err(failure),
    };
    // Closing the route first revokes every unreleased effect and registration.
    // The state (including suffix/FIFO/receipt) has now been dropped. This await
    // retains the session's sole JoinHandle across cancellation-safe gate waits.
    // A force-aborted outer task still has only Session::drop's weaker boundary.
    let closed = owner.endpoint.close_releases();
    let joined = session.cancel_and_join().await.map_err(provider_error);
    match outcome {
        Err(mut original) => {
            if let Err(cleanup) = joined {
                original
                    .message
                    .push_str(&format!("; provider cleanup: {cleanup}"));
            }
            if let Err(cleanup) = closed {
                original
                    .message
                    .push_str(&format!("; release cleanup: {cleanup}"));
            }
            Err(original)
        }
        Ok(()) => {
            closed?;
            joined
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{prefix_plan, PrefixPlan, SourceDependency};

    #[test]
    fn incomplete_source_admits_punctuation_without_forcing_a_user_commit() {
        assert_eq!(
            prefix_plan(SourceDependency::Unfinalized, 3, 1, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: None,
            }
        );
    }

    #[test]
    fn no_output_dispatches_all_finals_without_forcing_a_user_message_boundary() {
        assert_eq!(
            prefix_plan(SourceDependency::NoHeldOutput, 3, 1, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: None,
            }
        );
        // All finals may already be admitted, awaiting an actual output or End
        // barrier. Keeping that prefix staged must not manufacture an ACK flush.
        assert_eq!(
            prefix_plan(SourceDependency::NoHeldOutput, 3, 3, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: None,
            }
        );
    }

    #[test]
    fn final_end_barrier_selects_all_remaining_actual_finals() {
        assert_eq!(
            prefix_plan(SourceDependency::EndOfInput, 3, 1, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: Some(3),
            }
        );
    }

    #[test]
    fn completed_source_selects_a_prefix_and_leaves_newer_finals_queued() {
        assert_eq!(
            prefix_plan(SourceDependency::Finalized(2), 4, 1, 0),
            PrefixPlan {
                admit_through: 2,
                commit_through: Some(2),
            }
        );
    }

    #[test]
    fn receipt_target_includes_all_actual_prior_admissions() {
        assert_eq!(
            prefix_plan(SourceDependency::Finalized(1), 4, 3, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: Some(3),
            }
        );
        // A whitespace-only suffix or interruption can leave no queued final
        // even though its already-admitted nonempty prefix still needs a receipt.
        assert_eq!(
            prefix_plan(SourceDependency::EndOfInput, 3, 3, 0),
            PrefixPlan {
                admit_through: 3,
                commit_through: Some(3),
            }
        );
    }

    #[test]
    fn already_committed_or_no_content_prefix_does_not_request_a_fake_receipt() {
        assert_eq!(
            prefix_plan(SourceDependency::Finalized(0), 0, 0, 0),
            PrefixPlan {
                admit_through: 0,
                commit_through: None,
            }
        );
        assert_eq!(
            prefix_plan(SourceDependency::EndOfInput, 3, 3, 3),
            PrefixPlan {
                admit_through: 3,
                commit_through: None,
            }
        );
    }
}
