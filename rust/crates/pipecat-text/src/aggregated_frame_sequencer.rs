//! Ordered, bounded aggregate slots with generation-scoped provider-word routing.
//!
//! Register completed aggregate units here. [`crate::TokenFrameSequencer`] adds
//! token-to-sentence promotion. Provider audio/playout synchronization belongs
//! to a service adapter. No event
//! emitted by this utility is permission to append to audible spoken history.
use crate::{
    AlignmentError, AlignmentEvidence, AlignmentLimits, WordCompletion, WordCompletionTracker,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(0);

/// An opaque context identity issued by one sequencer generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ContextId {
    owner: u64,
    generation: u64,
    serial: u64,
}
impl ContextId {
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn serial(self) -> u64 {
        self.serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SegmentId {
    owner: u64,
    generation: u64,
    serial: u64,
}
impl SegmentId {
    pub fn generation(self) -> u64 {
        self.generation
    }
    pub fn serial(self) -> u64 {
        self.serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequencerLimits {
    pub max_contexts: usize,
    pub max_slots: usize,
    /// Sum of synthesis, display and LLM source bytes retained by pending slots.
    pub max_pending_text_bytes: usize,
    pub alignment: AlignmentLimits,
}
impl Default for SequencerLimits {
    fn default() -> Self {
        Self {
            max_contexts: 32,
            max_slots: 256,
            max_pending_text_bytes: 1_048_576,
            alignment: AlignmentLimits::default(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequencerEvent {
    /// Provider text reconstruction. `append_to_context` is application intent,
    /// never a claim that synthesis was submitted, played, or heard.
    Word {
        context: ContextId,
        segment: SegmentId,
        pts_ns: u64,
        completion: WordCompletion,
        append_to_context: bool,
        includes_inter_frame_spaces: bool,
    },
    /// Missing provider reports, exposed only for an explicit application
    /// reconstruction policy. This is never an ordinary word event.
    Unreported {
        context: ContextId,
        segment: SegmentId,
        pts_ns: u64,
        completion: WordCompletion,
        append_to_context: bool,
        includes_inter_frame_spaces: bool,
    },
    /// Text deliberately excluded from synthesis, released in aggregate order.
    Skipped {
        context: ContextId,
        segment: SegmentId,
        text: String,
        raw_text: String,
        transport_destination: Option<String>,
        pts_ns: Option<u64>,
    },
    /// A current-context token did not match any eligible slot. No cursor moved.
    UnmatchedWord {
        context: ContextId,
        word: String,
        pts_ns: u64,
    },
}
#[derive(Clone, Debug)]
struct Slot {
    context: ContextId,
    id: SegmentId,
    kind: SlotKind,
    bytes: usize,
}
#[derive(Clone, Debug)]
enum SlotKind {
    Tracked {
        tracker: Box<WordCompletionTracker>,
        complete: bool,
        includes_inter_frame_spaces: bool,
    },
    Skipped {
        text: String,
        raw_text: String,
        destination: Option<String>,
    },
}
#[derive(Debug)]
pub struct AggregatedFrameSequencer {
    owner: u64,
    generation: u64,
    next_context: u64,
    next_segment: u64,
    limits: SequencerLimits,
    contexts: HashMap<ContextId, bool>,
    slots: VecDeque<Slot>,
    pending_bytes: usize,
}
impl AggregatedFrameSequencer {
    // Private transactional checkpoints preserve owner IDs without exposing a
    // clonable public sequencer whose two instances could issue colliding IDs.
    pub(crate) fn checkpoint(&self) -> Self {
        Self {
            owner: self.owner,
            generation: self.generation,
            next_context: self.next_context,
            next_segment: self.next_segment,
            limits: self.limits,
            contexts: self.contexts.clone(),
            slots: self.slots.clone(),
            pending_bytes: self.pending_bytes,
        }
    }
    pub(crate) fn segment_is_pending(&self, id: SegmentId) -> bool {
        self.slots.iter().any(|s| {
            s.id == id
                && matches!(
                    s.kind,
                    SlotKind::Tracked {
                        complete: false,
                        ..
                    }
                )
        })
    }
    /// Completed spoken slots never block ordering. A streaming close can retire
    /// its bounded working slot even when an earlier context still blocks the
    /// queue; skipped slots must keep their positions until that barrier clears.
    pub(crate) fn retire_closed_context_slots(&mut self, context: ContextId) {
        self.slots.retain(|slot| {
            let retire = slot.context == context
                && matches!(slot.kind, SlotKind::Tracked { complete: true, .. });
            if retire {
                self.pending_bytes -= slot.bytes;
            }
            !retire
        });
    }
    /// Explicit no-timestamp completion of one exact admitted aggregate.
    pub(crate) fn complete_segment_unreported(
        &mut self,
        context: ContextId,
        segment: SegmentId,
        pts_ns: u64,
    ) -> Result<Vec<SequencerEvent>, AlignmentError> {
        self.check_context(context)?;
        let index = self
            .slots
            .iter()
            .position(|s| s.context == context && s.id == segment)
            .ok_or(AlignmentError::UnknownSegment)?;
        if !matches!(
            self.slots[index].kind,
            SlotKind::Tracked {
                complete: false,
                ..
            }
        ) {
            return Err(AlignmentError::UnknownSegment);
        }
        if self.active(context, 0) != Some(index) {
            return Err(AlignmentError::UnsupportedOperation(
                "synthesis completion out of order",
            ));
        }
        let slot = &mut self.slots[index];
        let SlotKind::Tracked {
            tracker,
            complete: false,
            includes_inter_frame_spaces,
        } = &slot.kind
        else {
            return Err(AlignmentError::UnknownSegment);
        };
        let spaces = *includes_inter_frame_spaces;
        let mut tracker = tracker.clone();
        let completion = tracker.complete_unreported(self.generation, None)?;
        slot.kind = SlotKind::Tracked {
            tracker,
            complete: true,
            includes_inter_frame_spaces: spaces,
        };
        let mut output = Vec::new();
        if completion.word.as_deref().is_some_and(|s| !s.is_empty())
            || completion
                .source_text
                .as_deref()
                .is_some_and(|s| !s.is_empty())
        {
            output.push(SequencerEvent::Unreported {
                context,
                segment,
                pts_ns,
                completion,
                append_to_context: self.contexts[&context],
                includes_inter_frame_spaces: spaces,
            });
        }
        output.extend(self.flush(Some(pts_ns)));
        Ok(output)
    }
    /// IDs are scoped to this instance; do not route them between instances.
    pub fn new(generation: u64, limits: SequencerLimits) -> Result<Self, AlignmentError> {
        if limits.max_contexts == 0 || limits.max_slots == 0 || limits.max_pending_text_bytes == 0 {
            return Err(AlignmentError::InvalidLimits);
        }
        WordCompletionTracker::new(generation, "", None, None, limits.alignment)?;
        let owner = NEXT_OWNER
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| AlignmentError::IdentityExhausted)?;
        Ok(Self {
            owner,
            generation,
            next_context: 0,
            next_segment: 0,
            limits,
            contexts: HashMap::new(),
            slots: VecDeque::new(),
            pending_bytes: 0,
        })
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn pending_slots(&self) -> usize {
        self.slots.len()
    }
    pub fn pending_text_bytes(&self) -> usize {
        self.pending_bytes
    }
    pub fn live_contexts(&self) -> usize {
        self.contexts.len()
    }
    pub fn open_context(&mut self, append_to_context: bool) -> Result<ContextId, AlignmentError> {
        if self.contexts.len() >= self.limits.max_contexts {
            return Err(AlignmentError::LimitExceeded("contexts"));
        }
        let next = self
            .next_context
            .checked_add(1)
            .ok_or(AlignmentError::IdentityExhausted)?;
        let context = ContextId {
            owner: self.owner,
            generation: self.generation,
            serial: self.next_context,
        };
        self.next_context = next;
        self.contexts.insert(context, append_to_context);
        Ok(context)
    }
    pub(crate) fn check_context(&self, context: ContextId) -> Result<(), AlignmentError> {
        if context.owner != self.owner {
            return Err(AlignmentError::UnknownContext);
        }
        if context.generation != self.generation {
            return Err(AlignmentError::StaleGeneration);
        }
        if !self.contexts.contains_key(&context) {
            return Err(AlignmentError::ContextClosed);
        }
        Ok(())
    }
    fn check_slot_budget(&self, bytes: usize) -> Result<(), AlignmentError> {
        if self.slots.len() >= self.limits.max_slots {
            return Err(AlignmentError::LimitExceeded("slots"));
        }
        if self
            .pending_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > self.limits.max_pending_text_bytes)
        {
            return Err(AlignmentError::LimitExceeded("pending text bytes"));
        }
        Ok(())
    }
    fn issue_segment(&mut self) -> Result<SegmentId, AlignmentError> {
        let next = self
            .next_segment
            .checked_add(1)
            .ok_or(AlignmentError::IdentityExhausted)?;
        let id = SegmentId {
            owner: self.owner,
            generation: self.generation,
            serial: self.next_segment,
        };
        self.next_segment = next;
        Ok(id)
    }
    pub fn register_spoken(
        &mut self,
        context: ContextId,
        tts_text: &str,
        user_facing_text: &str,
        llm_text: &str,
        includes_inter_frame_spaces: bool,
    ) -> Result<SegmentId, AlignmentError> {
        self.check_context(context)?;
        let bytes = tts_text
            .len()
            .checked_add(user_facing_text.len())
            .and_then(|n| n.checked_add(llm_text.len()))
            .ok_or(AlignmentError::LimitExceeded("pending text bytes"))?;
        self.check_slot_budget(bytes)?;
        let tracker = WordCompletionTracker::new(
            self.generation,
            tts_text,
            Some(llm_text),
            Some(user_facing_text),
            self.limits.alignment,
        )?;
        let id = self.issue_segment()?;
        self.slots.push_back(Slot {
            context,
            id,
            bytes,
            kind: SlotKind::Tracked {
                tracker: Box::new(tracker),
                complete: false,
                includes_inter_frame_spaces,
            },
        });
        self.pending_bytes += bytes;
        Ok(id)
    }
    pub fn register_skipped(
        &mut self,
        context: ContextId,
        text: &str,
        raw_text: &str,
        destination: Option<&str>,
    ) -> Result<(SegmentId, Vec<SequencerEvent>), AlignmentError> {
        self.check_context(context)?;
        if [text, raw_text, destination.unwrap_or("")]
            .iter()
            .any(|s| s.len() > self.limits.alignment.max_text_bytes)
        {
            return Err(AlignmentError::LimitExceeded("text bytes"));
        }
        let bytes = text
            .len()
            .checked_add(raw_text.len())
            .and_then(|n| n.checked_add(destination.map_or(0, str::len)))
            .ok_or(AlignmentError::LimitExceeded("pending text bytes"))?;
        self.check_slot_budget(bytes)?;
        let id = self.issue_segment()?;
        self.slots.push_back(Slot {
            context,
            id,
            bytes,
            kind: SlotKind::Skipped {
                text: text.into(),
                raw_text: raw_text.into(),
                destination: destination.map(str::to_owned),
            },
        });
        self.pending_bytes += bytes;
        Ok((id, self.flush(None)))
    }
    fn active(&self, context: ContextId, after: usize) -> Option<usize> {
        self.slots
            .iter()
            .enumerate()
            .skip(after)
            .find_map(|(i, s)| {
                (s.context == context
                    && matches!(
                        s.kind,
                        SlotKind::Tracked {
                            complete: false,
                            ..
                        }
                    ))
                .then_some(i)
            })
    }
    fn fits(&self, index: usize, word: &str) -> Result<bool, AlignmentError> {
        match &self.slots[index].kind {
            SlotKind::Tracked { tracker, .. } => tracker.word_belongs_here(word),
            _ => Ok(false),
        }
    }
    pub fn process_word(
        &mut self,
        context: ContextId,
        word: &str,
        pts_ns: u64,
        includes_inter_frame_spaces: bool,
    ) -> Result<Vec<SequencerEvent>, AlignmentError> {
        self.check_context(context)?;
        if word.len() > self.limits.alignment.max_word_bytes {
            return Err(AlignmentError::LimitExceeded("word bytes"));
        }
        let mut word = word.to_owned();
        let mut output = Vec::new();
        let append = self.contexts[&context];
        loop {
            let Some(index) = self.active(context, 0) else {
                if !word.is_empty() {
                    output.push(SequencerEvent::UnmatchedWord {
                        context,
                        word,
                        pts_ns,
                    });
                }
                break;
            };
            if !self.fits(index, &word)? {
                let next_fits = self
                    .active(context, index + 1)
                    .map(|i| self.fits(i, &word))
                    .transpose()?
                    .unwrap_or(false);
                if !next_fits {
                    output.push(SequencerEvent::UnmatchedWord {
                        context,
                        word,
                        pts_ns,
                    });
                    break;
                }
            }
            let slot = &mut self.slots[index];
            let SlotKind::Tracked {
                tracker,
                complete,
                includes_inter_frame_spaces: slot_spaces,
            } = &mut slot.kind
            else {
                unreachable!()
            };
            *slot_spaces |= includes_inter_frame_spaces;
            let completion = tracker.add_word(self.generation, &word)?;
            let overflow = completion.overflow.clone();
            *complete = completion.complete;
            if completion.word.as_deref().is_some_and(|s| !s.is_empty())
                || completion
                    .source_text
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
            {
                let event = if completion.evidence == AlignmentEvidence::UnreportedRemainder {
                    SequencerEvent::Unreported {
                        context,
                        segment: slot.id,
                        pts_ns,
                        completion,
                        append_to_context: append,
                        includes_inter_frame_spaces: *slot_spaces,
                    }
                } else {
                    SequencerEvent::Word {
                        context,
                        segment: slot.id,
                        pts_ns,
                        completion,
                        append_to_context: append,
                        includes_inter_frame_spaces: *slot_spaces,
                    }
                };
                output.push(event);
            }
            let finished = *complete;
            output.extend(self.flush(Some(pts_ns)));
            match overflow {
                Some(next) if finished && !next.is_empty() => word = next,
                _ => break,
            }
        }
        Ok(output)
    }
    /// End provider reporting. Missing suffixes stay `UnreportedRemainder`;
    /// closing or draining audio is not inferred here.
    pub fn close_context(
        &mut self,
        context: ContextId,
        pts_ns: u64,
    ) -> Result<Vec<SequencerEvent>, AlignmentError> {
        self.check_context(context)?;
        let append = self.contexts[&context];
        let mut output = Vec::new();
        while let Some(index) = self.active(context, 0) {
            let slot = &mut self.slots[index];
            let SlotKind::Tracked {
                tracker,
                complete,
                includes_inter_frame_spaces,
            } = &mut slot.kind
            else {
                unreachable!()
            };
            let completion = tracker.complete_unreported(self.generation, None)?;
            *complete = true;
            if completion.word.as_deref().is_some_and(|s| !s.is_empty())
                || completion
                    .source_text
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
            {
                debug_assert_eq!(completion.evidence, AlignmentEvidence::UnreportedRemainder);
                output.push(SequencerEvent::Unreported {
                    context,
                    segment: slot.id,
                    pts_ns,
                    completion,
                    append_to_context: append,
                    includes_inter_frame_spaces: *includes_inter_frame_spaces,
                });
            }
            output.extend(self.flush(Some(pts_ns)));
        }
        self.contexts.remove(&context);
        output.extend(self.flush(Some(pts_ns)));
        Ok(output)
    }
    fn flush(&mut self, pts_ns: Option<u64>) -> Vec<SequencerEvent> {
        let mut output = Vec::new();
        while self.slots.front().is_some_and(|s| {
            !matches!(
                s.kind,
                SlotKind::Tracked {
                    complete: false,
                    ..
                }
            )
        }) {
            let slot = self.slots.pop_front().unwrap();
            self.pending_bytes -= slot.bytes;
            if let SlotKind::Skipped {
                text,
                raw_text,
                destination,
            } = slot.kind
            {
                output.push(SequencerEvent::Skipped {
                    context: slot.context,
                    segment: slot.id,
                    text,
                    raw_text,
                    transport_destination: destination,
                    pts_ns,
                });
            }
        }
        output
    }
    /// Discard all pending state and invalidate every issued ID atomically.
    pub fn interrupt(&mut self) -> Result<u64, AlignmentError> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or(AlignmentError::IdentityExhausted)?;
        self.slots.clear();
        self.contexts.clear();
        self.pending_bytes = 0;
        self.generation = generation;
        Ok(generation)
    }
}
