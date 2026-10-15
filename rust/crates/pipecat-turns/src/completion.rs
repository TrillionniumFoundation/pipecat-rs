//! Source marker protocol with bounded retention and caller-owned monotonic time.
//!
//! This stage filters unsigned conversational text. Speculative inference and
//! tool-effect admission remain caller-owned. See `COMPLETION.md`.
mod runtime;
pub(crate) mod transcript;
pub use runtime::LLMTurnCompletionProcessor;

use crate::{AggregationOutput, RecentFrames, UserTurnStopStrategy};
use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, LlmMarkerFrame, LlmMarkerResponseFrame,
    MessagesFrame, ProcessorError,
};
use std::{collections::VecDeque, time::Duration};

const MAX_BUFFERED_FRAMES: usize = 256;
const MAX_BUFFERED_ENVELOPE_BYTES: usize = 64 * 1024;

pub const USER_TURN_COMPLETE_MARKER: &str = "●";
pub const USER_TURN_INCOMPLETE_SHORT_MARKER: &str = "◐";
pub const USER_TURN_INCOMPLETE_LONG_MARKER: &str = "○";

/// Provider-independent configuration. Prompts follow the configured markers;
/// callers explicitly compose `completion_instructions()` into their LLM request.
#[derive(Debug, Clone)]
pub struct LLMTurnCompletionConfig {
    pub complete_marker: String,
    pub incomplete_short_marker: String,
    pub incomplete_long_marker: String,
    pub instructions: Option<String>,
    pub incomplete_short_prompt: Option<String>,
    pub incomplete_long_prompt: Option<String>,
    pub incomplete_short_timeout: Duration,
    pub incomplete_long_timeout: Duration,
    pub max_prefix_bytes: usize,
    pub max_raw_bytes: usize,
    /// Limits automatic check-ins until actual user speech or an explicit run.
    pub max_reprompts_per_segment: usize,
}
impl Default for LLMTurnCompletionConfig {
    fn default() -> Self {
        Self {
            complete_marker: USER_TURN_COMPLETE_MARKER.into(),
            incomplete_short_marker: USER_TURN_INCOMPLETE_SHORT_MARKER.into(),
            incomplete_long_marker: USER_TURN_INCOMPLETE_LONG_MARKER.into(),
            instructions: None,
            incomplete_short_prompt: None,
            incomplete_long_prompt: None,
            incomplete_short_timeout: Duration::from_secs(5),
            incomplete_long_timeout: Duration::from_secs(10),
            max_prefix_bytes: 16 * 1024,
            max_raw_bytes: 64 * 1024,
            max_reprompts_per_segment: 1,
        }
    }
}
impl TryFrom<pipecat_core::UserTurnCompletionConfig> for LLMTurnCompletionConfig {
    type Error = ProcessorError;
    fn try_from(config: pipecat_core::UserTurnCompletionConfig) -> Result<Self, Self::Error> {
        fn timeout(seconds: f64) -> Result<Duration, ProcessorError> {
            if !seconds.is_finite() || seconds <= 0.0 || seconds > 86_400.0 {
                return Err(ProcessorError::new(
                    "completion timeouts must be finite, positive, and at most 86400 seconds",
                ));
            }
            Duration::try_from_secs_f64(seconds).map_err(|e| ProcessorError::new(e.to_string()))
        }
        let result = Self {
            complete_marker: config.complete_marker,
            incomplete_short_marker: config.incomplete_short_marker,
            incomplete_long_marker: config.incomplete_long_marker,
            instructions: config.instructions,
            incomplete_short_prompt: config.incomplete_short_prompt,
            incomplete_long_prompt: config.incomplete_long_prompt,
            incomplete_short_timeout: timeout(config.incomplete_short_timeout)?,
            incomplete_long_timeout: timeout(config.incomplete_long_timeout)?,
            ..Default::default()
        };
        result.validate()?;
        Ok(result)
    }
}
impl LLMTurnCompletionConfig {
    pub fn markers(&self) -> [&str; 3] {
        [
            &self.complete_marker,
            &self.incomplete_short_marker,
            &self.incomplete_long_marker,
        ]
    }
    pub fn completion_instructions(&self) -> String {
        self.instructions
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                self.render(include_str!("completion/instructions.txt").trim_end_matches('\n'))
            })
    }
    pub fn short_prompt(&self) -> String {
        self.incomplete_short_prompt
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                self.render(include_str!("completion/short_prompt.txt").trim_end_matches('\n'))
            })
    }
    pub fn long_prompt(&self) -> String {
        self.incomplete_long_prompt
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                self.render(include_str!("completion/long_prompt.txt").trim_end_matches('\n'))
            })
    }
    fn render(&self, template: &str) -> String {
        // One pass: marker text containing a placeholder is never substituted again.
        let mut result = String::new();
        let mut rest = template;
        while let Some(index) = rest.find('{') {
            result.push_str(&rest[..index]);
            rest = &rest[index..];
            let replacement = [
                ("{complete}", &self.complete_marker),
                ("{short}", &self.incomplete_short_marker),
                ("{long}", &self.incomplete_long_marker),
            ]
            .into_iter()
            .find(|(token, _)| rest.starts_with(token));
            if let Some((token, value)) = replacement {
                result.push_str(value);
                rest = &rest[token.len()..];
            } else {
                result.push('{');
                rest = &rest[1..];
            }
        }
        result.push_str(rest);
        result
    }
    fn validate(&self) -> Result<(), ProcessorError> {
        let markers = self.markers();
        for (i, marker) in markers.iter().enumerate() {
            if marker.is_empty() || marker.len() > 256 || marker.contains(char::is_whitespace) {
                return Err(ProcessorError::new(
                    "completion markers must be 1..=256 bytes without whitespace",
                ));
            }
            if markers
                .iter()
                .enumerate()
                .any(|(j, other)| i != j && (marker.contains(other) || other.contains(marker)))
            {
                return Err(ProcessorError::new(
                    "completion markers must be distinct and not contain one another",
                ));
            }
        }
        if self.max_prefix_bytes < markers.iter().map(|m| m.len()).max().unwrap()
            || self.max_prefix_bytes > 1024 * 1024
            || self.max_raw_bytes == 0
            || self.max_raw_bytes > 1024 * 1024
            || self.max_reprompts_per_segment > 64
        {
            return Err(ProcessorError::new(
                "invalid completion retention or retry limits",
            ));
        }
        for timeout in [self.incomplete_short_timeout, self.incomplete_long_timeout] {
            if timeout.is_zero() || timeout > Duration::from_secs(86_400) {
                return Err(ProcessorError::new(
                    "completion timeouts must be positive and at most 86400 seconds",
                ));
            }
        }
        for text in [
            &self.instructions,
            &self.incomplete_short_prompt,
            &self.incomplete_long_prompt,
        ]
        .into_iter()
        .flatten()
        {
            if text.len() > 64 * 1024 {
                return Err(ProcessorError::new("completion prompt exceeds 65536 bytes"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Complete,
    Short,
    Long,
    Fallback,
    Suppressed,
}
impl Verdict {
    fn kind(self) -> Option<&'static str> {
        match self {
            Self::Complete => Some("complete"),
            Self::Short => Some("short"),
            Self::Long => Some("long"),
            _ => None,
        }
    }
}

/// A deterministic marker strategy for explicitly unsigned LLM response streams.
/// `process_frame` and `advance` never execute provider or tool work. End drains
/// an unmarked response; Stop, Cancel, and interruption discard it.
pub struct LLMTurnCompletionStrategy {
    config: LLMTurnCompletionConfig,
    prefix: String,
    buffered: VecDeque<Frame>,
    buffered_envelope_bytes: usize,
    raw: String,
    raw_truncated: bool,
    prefix_exceeded: bool,
    malformed: bool,
    marker_tail: String,
    marker_count: usize,
    verdict: Option<Verdict>,
    response_seen: bool,
    accepts_text: bool,
    voiced: bool,
    completion_broadcasted: bool,
    // System tool-start may overtake this response's normal start/text. Keep
    // its permission until the ordered end, after all same-response text.
    tool_pending_until_end: bool,
    speaking: bool,
    waiting: Option<Verdict>,
    deadline: Option<Duration>,
    now: Duration,
    reprompts: usize,
    closed: bool,
    retry_epoch: u64,
    seen: RecentFrames,
}
impl LLMTurnCompletionStrategy {
    pub fn new(config: LLMTurnCompletionConfig) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config,
            prefix: String::new(),
            buffered: VecDeque::new(),
            buffered_envelope_bytes: 0,
            raw: String::new(),
            raw_truncated: false,
            prefix_exceeded: false,
            malformed: false,
            marker_tail: String::new(),
            marker_count: 0,
            verdict: None,
            response_seen: false,
            accepts_text: true,
            voiced: false,
            completion_broadcasted: false,
            tool_pending_until_end: false,
            speaking: false,
            waiting: None,
            deadline: None,
            now: Duration::ZERO,
            reprompts: 0,
            closed: false,
            retry_epoch: 0,
            seen: RecentFrames::default(),
        })
    }
    pub fn config(&self) -> &LLMTurnCompletionConfig {
        &self.config
    }
    /// Install with deferred detector policies. The application owns inference
    /// admission; this merely selects the existing completion-signal finalizer.
    pub fn stop_strategy(&self) -> UserTurnStopStrategy {
        UserTurnStopStrategy::external_completion()
    }
    pub fn deadline(&self) -> Option<Duration> {
        self.deadline
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn retained_text_bytes(&self) -> usize {
        self.prefix.len()
            + self.raw.len()
            + self.marker_tail.len()
            + self
                .buffered
                .iter()
                .map(|f| match &f.data {
                    FrameData::LlmText(t) => t.text.len(),
                    _ => 0,
                })
                .sum::<usize>()
    }
    pub fn stop(&mut self) {
        self.closed = true;
        self.accepts_text = false;
        self.cancel_deadline();
        self.reset_response();
        self.tool_pending_until_end = false;
    }
    fn cancel_deadline(&mut self) {
        self.deadline = None;
        self.waiting = None;
    }
    fn fresh_segment(&mut self) {
        self.voiced = false;
        self.tool_pending_until_end = false;
        self.completion_broadcasted = false;
        self.reprompts = 0;
        self.cancel_deadline();
    }
    fn reset_response(&mut self) {
        self.completion_broadcasted = false;
        self.prefix.clear();
        self.raw.clear();
        self.marker_tail.clear();
        self.buffered.clear();
        self.buffered_envelope_bytes = 0;
        self.verdict = None;
        self.response_seen = false;
        self.raw_truncated = false;
        self.prefix_exceeded = false;
        self.malformed = false;
        self.marker_count = 0;
    }
    fn arm(&mut self, verdict: Verdict) {
        if self.tool_pending_until_end {
            self.cancel_deadline();
            return;
        }
        self.waiting = Some(verdict);
        self.deadline = if self.speaking || self.reprompts >= self.config.max_reprompts_per_segment
        {
            None
        } else {
            self.now.checked_add(match verdict {
                Verdict::Long => self.config.incomplete_long_timeout,
                _ => self.config.incomplete_short_timeout,
            })
        };
    }
    pub fn process_frame(
        &mut self,
        frame: Frame,
        direction: Direction,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut output = AggregationOutput::default();
        // Reject even an empty start carrier: future chunks may need signed replay.
        match frame.native_llm_continuation() {
            Ok(None) => {}
            Ok(Some(_)) => {
                self.stop();
                return Err(ProcessorError::fatal("turn completion filtering does not support native provider continuation carriers"));
            }
            Err(error) => {
                self.stop();
                return Err(ProcessorError::fatal(error.to_string()));
            }
        }
        if self.closed {
            if is_terminal(&frame.data) {
                output.push(frame, direction);
            }
            return Ok(output);
        }
        self.now = self.now.max(now);
        if direction == Direction::Downstream
            && relevant(&frame.data)
            && !self.seen.insert(frame.id)
        {
            return Ok(output);
        }
        if invalidates_retry(&frame.data, direction) {
            let Some(epoch) = self.retry_epoch.checked_add(1) else {
                self.stop();
                return Err(ProcessorError::fatal("completion epoch exhausted"));
            };
            self.retry_epoch = epoch;
        }
        match &frame.data {
            FrameData::Interruption => {
                self.cancel_deadline();
                self.reset_response();
                self.voiced = false;
                self.tool_pending_until_end = false;
                self.accepts_text = false;
            }
            FrameData::UserStartedSpeaking => self.fresh_segment(),
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_) => {
                self.speaking = true;
                self.fresh_segment();
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                if self.speaking {
                    self.speaking = false;
                    if let Some(verdict) = self.waiting {
                        self.arm(verdict);
                    }
                }
            }
            FrameData::LlmRun => self.fresh_segment(),
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_) => self.cancel_deadline(),
            FrameData::LlmMessagesAppend(messages) if messages.run_llm => self.fresh_segment(),
            FrameData::LlmMessagesAppendDetailed(messages) if messages.run_llm == Some(true) => {
                self.fresh_segment()
            }
            FrameData::End(_) | FrameData::EndDetailed(_) => {
                self.finish_response(&mut output);
                self.stop();
            }
            FrameData::Stop | FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.stop(),
            data if direction == Direction::Downstream && tool_commitment(data) => {
                self.cancel_deadline();
                self.broadcast_completion(&mut output);
                self.voiced = false;
                self.tool_pending_until_end = true;
            }
            FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_)
                if direction == Direction::Downstream =>
            {
                self.cancel_deadline();
                let tool_broadcast = self.tool_pending_until_end && self.completion_broadcasted;
                self.reset_response();
                self.completion_broadcasted = tool_broadcast;
                self.accepts_text = true;
            }
            FrameData::LlmResponseEnd | FrameData::LlmFullResponseEnd(_)
                if direction == Direction::Downstream =>
            {
                self.finish_response(&mut output);
                self.accepts_text = false;
            }
            FrameData::LlmText(_) if direction == Direction::Downstream => {
                if self.accepts_text {
                    self.text(frame, &mut output);
                }
                return Ok(output);
            }
            _ => {}
        }
        output.push(frame, direction);
        Ok(output)
    }
    /// Consume the current deadline once. The appended developer message requests
    /// one run atomically at the assistant aggregator downstream of this strategy.
    pub fn advance(&mut self, now: Duration) -> Result<AggregationOutput, ProcessorError> {
        self.now = self.now.max(now);
        let mut output = AggregationOutput::default();
        if self.closed || self.speaking || !self.deadline.is_some_and(|d| self.now >= d) {
            return Ok(output);
        }
        let verdict = self.waiting.take().expect("deadline has verdict");
        self.deadline = None;
        self.reprompts += 1;
        // Never pretend the provider ended: late chunks could corrupt a retry.
        if self.response_seen {
            self.stop();
            return Err(ProcessorError::fatal(
                "incomplete LLM response did not end before its completion deadline",
            ));
        }
        self.voiced = false;
        let prompt = if verdict == Verdict::Long {
            self.config.long_prompt()
        } else {
            self.config.short_prompt()
        };
        output.push(
            Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
                messages: vec![ChatMessage::new(ChatRole::Developer, prompt)],
                run_llm: true,
            })),
            Direction::Downstream,
        );
        Ok(output)
    }
    fn text(&mut self, frame: Frame, output: &mut AggregationOutput) {
        let FrameData::LlmText(text) = &frame.data else {
            unreachable!()
        };
        if text.text.is_empty() {
            return;
        }
        self.response_seen = true;
        self.record_raw(&text.text);
        if self.voiced && self.verdict.is_none() {
            self.verdict = Some(Verdict::Suppressed);
        }
        match self.verdict {
            Some(Verdict::Complete | Verdict::Fallback) => {
                output.push(frame, Direction::Downstream);
                return;
            }
            Some(_) => return,
            None => {}
        }
        let room = self.config.max_prefix_bytes - self.prefix.len();
        let copied = char_boundary(&text.text, room.min(text.text.len()));
        self.prefix.push_str(&text.text[..copied]);
        let detected = self
            .config
            .markers()
            .iter()
            .enumerate()
            .find_map(|(index, marker)| {
                self.prefix
                    .starts_with(marker)
                    .then_some((index, marker.len()))
            });
        if let Some((index, length)) = detected {
            let verdict = match index {
                0 if !self.speaking => Verdict::Complete,
                2 => Verdict::Long,
                _ => Verdict::Short,
            };
            self.verdict = Some(verdict);
            if verdict == Verdict::Complete {
                self.voiced = true;
                self.broadcast_completion(output);
            }
            let marker = match verdict {
                Verdict::Complete => &self.config.complete_marker,
                Verdict::Long => &self.config.incomplete_long_marker,
                _ => &self.config.incomplete_short_marker,
            };
            output.push(
                Frame::new(FrameData::LlmMarker(LlmMarkerFrame {
                    marker: marker.clone(),
                    append_to_context_immediately: verdict != Verdict::Complete,
                })),
                Direction::Downstream,
            );
            if verdict == Verdict::Complete {
                self.buffered.push_back(frame);
                self.flush_prefix(output, length, true);
            } else {
                self.arm(verdict);
            }
            self.prefix.clear();
            self.buffered.clear();
            self.buffered_envelope_bytes = 0;
        } else if copied < text.text.len()
            || self.buffered.len() >= MAX_BUFFERED_FRAMES
            || envelope_bytes(&frame).is_none_or(|bytes| {
                bytes > MAX_BUFFERED_ENVELOPE_BYTES.saturating_sub(self.buffered_envelope_bytes)
            })
        {
            self.prefix_exceeded = true;
            self.malformed = true;
            self.verdict = Some(Verdict::Fallback);
            self.buffered.push_back(frame);
            self.flush_prefix(output, 0, false);
            self.prefix.clear();
        } else {
            self.buffered_envelope_bytes += envelope_bytes(&frame).expect("checked envelope size");
            self.buffered.push_back(frame);
        }
    }
    fn flush_prefix(
        &mut self,
        output: &mut AggregationOutput,
        mut skip: usize,
        mut trim_space: bool,
    ) {
        for mut frame in self.buffered.drain(..) {
            let FrameData::LlmText(text) = &mut frame.data else {
                unreachable!()
            };
            let consumed = skip.min(text.text.len());
            text.text.drain(..consumed);
            skip -= consumed;
            if text.text.is_empty() {
                continue;
            }
            if trim_space {
                if text.text.starts_with(' ') {
                    text.text.remove(0);
                }
                trim_space = false;
            }
            if !text.text.is_empty() {
                output.push(frame, Direction::Downstream);
            }
        }
        self.buffered_envelope_bytes = 0;
    }
    fn record_raw(&mut self, text: &str) {
        let copied = char_boundary(
            text,
            (self.config.max_raw_bytes - self.raw.len()).min(text.len()),
        );
        self.raw.push_str(&text[..copied]);
        self.raw_truncated |= copied < text.len();
        // Count only newly completed marker occurrences across chunk boundaries.
        let tail_len = self.marker_tail.len();
        let scan = format!("{}{}", self.marker_tail, text);
        for marker in self.config.markers() {
            let newly_found = scan
                .match_indices(marker)
                .filter(|(at, _)| at + marker.len() > tail_len)
                .count();
            self.marker_count = self.marker_count.saturating_add(newly_found).min(2);
        }
        let retain = self.config.markers().iter().map(|s| s.len()).max().unwrap() - 1;
        let mut start = scan.len().saturating_sub(retain);
        while !scan.is_char_boundary(start) {
            start += 1;
        }
        self.marker_tail = scan[start..].to_owned();
    }
    fn broadcast_completion(&mut self, output: &mut AggregationOutput) {
        if !self.completion_broadcasted {
            self.completion_broadcasted = true;
            output.broadcast(FrameData::UserTurnInferenceCompleted);
        }
    }
    fn finish_response(&mut self, output: &mut AggregationOutput) {
        if self.tool_pending_until_end {
            self.voiced = false;
            self.tool_pending_until_end = false;
            self.cancel_deadline();
        }
        if !self.response_seen {
            self.reset_response();
            return;
        }
        if self.verdict.is_none() && !self.prefix.is_empty() {
            self.malformed = true;
        }
        let marker = match self.verdict {
            Some(Verdict::Complete) => Some(self.config.complete_marker.clone()),
            Some(Verdict::Short) => Some(self.config.incomplete_short_marker.clone()),
            Some(Verdict::Long) => Some(self.config.incomplete_long_marker.clone()),
            _ => None,
        };
        let mut report = Frame::new(FrameData::LlmMarkerResponse(LlmMarkerResponseFrame {
            raw: std::mem::take(&mut self.raw),
            marker,
            kind: self.verdict.and_then(Verdict::kind).map(str::to_owned),
            markers: self.config.markers().map(str::to_owned).into(),
        }));
        report
            .metadata
            .insert("completion_raw_truncated".into(), self.raw_truncated.into());
        report.metadata.insert(
            "completion_prefix_exceeded".into(),
            self.prefix_exceeded.into(),
        );
        report.metadata.insert(
            "completion_malformed".into(),
            (self.malformed || self.marker_count > 1).into(),
        );
        report.metadata.insert(
            "completion_extra_markers".into(),
            (self.marker_count > 1).into(),
        );
        report.metadata.insert(
            "completion_suppressed".into(),
            (self.verdict == Some(Verdict::Suppressed)).into(),
        );
        output.push(report, Direction::Downstream);
        if self.verdict.is_none() && !self.prefix.is_empty() {
            self.flush_prefix(output, 0, false);
        }
        self.reset_response();
    }
}
fn envelope_bytes(frame: &Frame) -> Option<usize> {
    // Bound retained routing metadata without serializing local frame capabilities.
    let metadata = serde_json::to_vec(&frame.metadata).ok()?.len();
    metadata
        .checked_add(frame.name.len())?
        .checked_add(frame.transport_source.as_ref().map_or(0, String::len))?
        .checked_add(frame.transport_destination.as_ref().map_or(0, String::len))?
        .checked_add(std::mem::size_of::<Frame>())
}
fn char_boundary(text: &str, mut end: usize) -> usize {
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    end
}
fn is_terminal(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
    )
}
fn relevant(data: &FrameData) -> bool {
    tool_commitment(data)
        || matches!(
            data,
            FrameData::LlmText(_)
                | FrameData::LlmResponseStart
                | FrameData::LlmResponseEnd
                | FrameData::LlmFullResponseStart(_)
                | FrameData::LlmFullResponseEnd(_)
        )
}
fn tool_commitment(data: &FrameData) -> bool {
    match data {
        FrameData::FunctionCallsStarted(calls) => !calls.is_empty(),
        FrameData::FunctionCallsStartedDetailed(calls) => !calls.is_empty(),
        _ => false,
    }
}
fn invalidates_retry(data: &FrameData, direction: Direction) -> bool {
    (direction == Direction::Downstream && tool_commitment(data))
        || matches!(
            data,
            FrameData::Interruption
                | FrameData::UserStartedSpeaking
                | FrameData::LlmRun
                | FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                | FrameData::LlmContext(_)
                | FrameData::LlmContextDetailed(_)
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        )
        || (direction == Direction::Downstream
            && matches!(
                data,
                FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_)
            ))
        || matches!(data, FrameData::LlmMessagesAppend(m) if m.run_llm)
        || matches!(data, FrameData::LlmMessagesAppendDetailed(m) if m.run_llm == Some(true))
}
