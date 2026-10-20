//! Native paired-context and final-tool adapter for Azure Voice Live.
//! The distinct socket actor owns wire ordering, readiness and request debt.
use crate::{
    Action, ConversationItem, Event, PlaybackPosition, VoiceLiveClient, VoiceLiveError,
    VoiceLiveEvent, VoiceLiveSender, VoiceLiveSession,
};
use pipecat_core::{
    async_trait, AudioRawFrame, ChatRole, ContextMessage, Direction, ErrorFrame, Frame, FrameData,
    FrameProcessor, FunctionCall, LlmContextData, MessageContent, MessagePart, Metric,
    NativeRealtimeContextCause, ProcessorContext, ProcessorError, RealtimeContextAdmission,
    RealtimeContextIntent, StartFrame, TextFrame, TranscriptionFrame, TtsStartedFrame,
    TtsStoppedFrame,
};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

const MAX_HISTORY: usize = 1024;
const MAX_TRANSCRIPTS: usize = 512;
fn error(error: VoiceLiveError) -> ProcessorError {
    ProcessorError::fatal(error.to_string())
}
fn fatal(message: &'static str) -> ProcessorError {
    ProcessorError::fatal(message)
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<(), ProcessorError> {
    struct Budget(usize);
    impl Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("Voice Live context exceeds byte limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(limit), value)
        .map_err(|_| fatal("Voice Live context exceeds encoded byte limit"))
}
pub(crate) fn user_item(text: String) -> Result<ConversationItem, ProcessorError> {
    serde_json::from_value(
        json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]}),
    )
    .map_err(|_| fatal("Voice Live input-text item cannot be encoded"))
}
fn plain(message: &ContextMessage) -> Result<String, ProcessorError> {
    if !message.extra.is_empty() || message.name.is_some() {
        return Err(fatal(
            "Voice Live history cannot contain opaque extensions or names",
        ));
    }
    match &message.content {
        MessageContent::Text(text) => Ok(text.clone()),
        MessageContent::Parts(parts) => parts
            .iter()
            .map(|part| match part {
                MessagePart::Text { text } => Ok(text.as_str()),
                _ => Err(fatal("Voice Live history supports ordinary text only")),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| parts.join(" ")),
    }
}

#[derive(Clone)]
struct History {
    max_messages: usize,
    max_transcripts: usize,
    accepted: Vec<ContextMessage>,
    /// Item-keyed transcript identities remain for this bounded single session.
    /// Empty repeats do not consume or overwrite a final item claim.
    transcripts: BTreeMap<String, String>,
    assistant: BTreeMap<String, String>,
    calls: BTreeMap<String, FunctionCall>,
    results: BTreeMap<String, (u64, String)>,
    outcomes: BTreeSet<u64>,
}
impl Default for History {
    fn default() -> Self {
        Self {
            max_messages: MAX_HISTORY,
            max_transcripts: MAX_TRANSCRIPTS,
            accepted: Vec::new(),
            transcripts: BTreeMap::new(),
            assistant: BTreeMap::new(),
            calls: BTreeMap::new(),
            results: BTreeMap::new(),
            outcomes: BTreeSet::new(),
        }
    }
}
impl History {
    fn bound(&self, limit: usize) -> Result<(), ProcessorError> {
        if self.accepted.len() > self.max_messages.min(MAX_HISTORY)
            || self.calls.len() > MAX_HISTORY
            || self.results.len() > MAX_HISTORY
            || self.assistant.len() > MAX_HISTORY
            || self.transcripts.len() > self.max_transcripts.min(MAX_TRANSCRIPTS)
        {
            return Err(fatal(
                "Voice Live single-session history identity bound exceeded",
            ));
        }
        bounded(
            &(
                &self.accepted,
                &self.transcripts,
                &self.assistant,
                &self.calls,
                &self.results,
            ),
            limit,
        )
    }
    fn observe(&mut self, event: &Event, limit: usize) -> Result<bool, ProcessorError> {
        match event {
            Event::Text {
                response_id, delta, ..
            } => self
                .assistant
                .entry(response_id.clone())
                .or_default()
                .push_str(delta),
            Event::Transcript {
                item_id,
                text,
                is_final: true,
            } => {
                let text = text.trim();
                if text.is_empty() {
                    return Ok(false);
                }
                if let Some(old) = self.transcripts.get(item_id) {
                    if old != text {
                        return Err(fatal("Voice Live transcript changed its completed item"));
                    }
                    return Ok(false);
                }
                self.transcripts.insert(item_id.clone(), text.to_owned());
            }
            Event::ToolCall { call, .. } => {
                if let Some(old) = self.calls.get(&call.tool_call_id) {
                    if old != call {
                        return Err(fatal("Voice Live provider call identity changed"));
                    }
                    return Ok(false);
                }
                self.calls.insert(call.tool_call_id.clone(), call.clone());
            }
            _ => {}
        }
        self.bound(limit)?;
        Ok(true)
    }
    fn terminal(&mut self, frame: &Frame, limit: usize) -> Result<bool, ProcessorError> {
        let outcome = frame.native_tool_outcome()?.ok_or_else(|| {
            fatal("Voice Live final tool output requires an original native receipt")
        })?;
        if outcome.progress_terminal().is_some()
            || !outcome.cancel_on_interruption()
            || self.calls.get(&outcome.call().tool_call_id) != Some(outcome.call())
        {
            return Err(fatal(
                "Voice Live terminal does not match an admitted interruptible final call",
            ));
        }
        if self.outcomes.contains(&outcome.id()) {
            return Ok(false);
        }
        if self.results.contains_key(&outcome.call().tool_call_id) {
            return Err(fatal("Voice Live call has conflicting terminal receipts"));
        }
        let content = match &frame.data {
            FrameData::FunctionCallResult(result) => {
                if let Some(error) = &result.error {
                    json!({"error":error}).to_string()
                } else if result.result.is_null() {
                    "COMPLETED".into()
                } else {
                    result.result.to_string()
                }
            }
            FrameData::FunctionCallCancel(_) => "CANCELLED".into(),
            _ => {
                return Err(fatal(
                    "Voice Live requires an unchanged native terminal payload",
                ))
            }
        };
        self.results
            .insert(outcome.call().tool_call_id.clone(), (outcome.id(), content));
        self.outcomes.insert(outcome.id());
        self.bound(limit)?;
        Ok(true)
    }
    fn validate_new(&mut self, message: &ContextMessage) -> Result<(), ProcessorError> {
        let text = plain(message)?;
        match message.role {
            ChatRole::User if message.tool_call_id.is_none() && message.tool_calls.is_empty() => {
                // App callers cannot mutate the shared pair or inject context.
                // A trailing paired observation has no provider authority. Only
                // an explicit sealed Append range below can send user text.
                // No text-equality heuristic consumes a transcript item claim.
            }
            ChatRole::Assistant if message.tool_call_id.is_none() => {
                if !message.tool_calls.is_empty() {
                    if !text.is_empty()
                        || message
                            .tool_calls
                            .iter()
                            .any(|call| self.calls.get(&call.tool_call_id) != Some(call))
                    {
                        return Err(fatal(
                            "Voice Live assistant call history lacks provider ownership",
                        ));
                    }
                } else if !text.trim().is_empty() {
                    let mut found = false;
                    for pending in self.assistant.values_mut() {
                        let trimmed = pending.trim_start();
                        if trimmed.starts_with(text.trim()) {
                            *pending = trimmed[text.trim().len()..].to_owned();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Err(fatal(
                            "Voice Live assistant history lacks generated text ownership",
                        ));
                    }
                }
            }
            ChatRole::Tool if message.tool_calls.is_empty() => {
                let id = message
                    .tool_call_id
                    .as_ref()
                    .ok_or_else(|| fatal("Voice Live tool history lacks call identity"))?;
                if !self.calls.contains_key(id)
                    || (text != "IN_PROGRESS"
                        && self
                            .results
                            .get(id)
                            .is_none_or(|(_, result)| result != &text))
                {
                    return Err(fatal(
                        "Voice Live tool history lacks native terminal ownership",
                    ));
                }
            }
            _ => {
                return Err(fatal(
                    "Voice Live live history role or authority is unsupported",
                ))
            }
        }
        Ok(())
    }
    fn snapshot(
        &mut self,
        data: &LlmContextData,
        append: Option<(usize, usize)>,
        _state_only: bool,
        limit: usize,
    ) -> Result<Vec<Action>, ProcessorError> {
        bounded(data, limit)?;
        if data.messages.len() > self.max_messages.min(MAX_HISTORY) {
            return Err(fatal("Voice Live history message bound exceeded"));
        }
        // Both genuine paired producers may report older snapshots. Preserve
        // the accepted suffix; explicit Replace causes are rejected by caller.
        let mut actions = vec![];
        for (index, message) in data.messages.iter().enumerate() {
            plain(message)?;
            let explicit = append.is_some_and(|(start, end)| index >= start && index < end);
            if explicit {
                if message.role != ChatRole::User
                    || message.tool_call_id.is_some()
                    || !message.tool_calls.is_empty()
                {
                    return Err(fatal(
                        "Voice Live explicit appends require ordinary user text",
                    ));
                }
                if self.accepted.get(index).is_some_and(|old| old != message) {
                    return Err(fatal(
                        "Voice Live explicit append conflicts with accepted history",
                    ));
                }
                // The native producer sequence is the exactly-once authority.
                // A concurrent state-only observation of equal text cannot eat
                // this intentional user turn or its response request.
                actions.push(Action::CreateItem(user_item(plain(message)?)?));
            }
            if let Some(old) = self.accepted.get(index) {
                if old == message {
                    continue;
                }
                let same_tool = old.role == ChatRole::Tool
                    && message.role == ChatRole::Tool
                    && old.tool_call_id == message.tool_call_id;
                if same_tool && plain(message)? == "IN_PROGRESS" {
                    self.validate_new(message)?;
                    continue; // older snapshot cannot roll a terminal back
                }
                if !same_tool || plain(old)? != "IN_PROGRESS" {
                    return Err(fatal("Voice Live snapshot rewrites accepted history"));
                }
                self.validate_new(message)?;
                self.accepted[index] = message.clone();
                continue;
            }
            if !explicit {
                self.validate_new(message)?;
            }
            self.accepted.push(message.clone());
        }
        // Native context admission separately deduplicates the producer sequence.
        if let Some((start, end)) = append {
            if start > end || end > data.messages.len() {
                return Err(fatal("Voice Live append range is invalid"));
            }
        }
        self.bound(limit)?;
        Ok(actions)
    }
}

#[derive(Default)]
struct Mapper {
    active: Option<String>,
    speaking: Option<String>,
}
impl Mapper {
    fn close(&mut self) -> Vec<Frame> {
        let mut frames = vec![];
        if let Some(id) = self.speaking.take() {
            frames.push(Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some(id),
            })));
        }
        if self.active.take().is_some() {
            frames.push(Frame::new(FrameData::LlmResponseEnd));
        }
        frames
    }
    fn frames(
        &mut self,
        event: &VoiceLiveEvent,
        usage_metrics: bool,
        manual: bool,
    ) -> Result<Vec<(Frame, Direction)>, ProcessorError> {
        let mut frames = vec![];
        match &event.event {
            Event::Ready => {}
            Event::ResponseStarted { response_id } => {
                match &self.active {
                    Some(active) if active != response_id => {
                        return Err(fatal("Voice Live overlapping response lifecycle"));
                    }
                    Some(_) => {} // A function item may have opened this response.
                    None => {
                        self.active = Some(response_id.clone());
                        frames.push(Frame::new(FrameData::LlmResponseStart));
                    }
                }
            }
            Event::Audio {
                response_id, audio, ..
            } => {
                if self.active.as_ref() != Some(response_id) || audio.len() % 2 != 0 {
                    return Err(fatal("Voice Live audio lacks an active aligned response"));
                }
                if self.speaking.is_none() {
                    self.speaking = Some(response_id.clone());
                    frames.push(Frame::new(FrameData::TtsStarted(TtsStartedFrame {
                        context_id: Some(response_id.clone()),
                        ..Default::default()
                    })));
                }
                if !audio.is_empty() {
                    frames.push(Frame::new(FrameData::TtsAudio(AudioRawFrame {
                        audio: audio.clone(),
                        sample_rate: 24_000,
                        num_channels: 1,
                        context_id: Some(response_id.clone()),
                    })));
                }
            }
            Event::Text {
                response_id,
                delta,
                audio_transcript,
                ..
            } => {
                if self.active.as_ref() != Some(response_id) {
                    return Err(fatal("Voice Live text lacks an active response"));
                }
                if *audio_transcript {
                    let mut notification = TextFrame::llm(delta);
                    notification.append_to_context = false;
                    frames.push(Frame::new(FrameData::LlmText(notification)));
                    let mut text = TextFrame::llm(delta);
                    text.includes_inter_frame_spaces = true;
                    frames.push(Frame::new(FrameData::TtsText(
                        pipecat_core::AggregatedTextFrame {
                            text,
                            aggregated_by: "sentence".into(),
                            context_id: Some(response_id.clone()),
                            raw_text: None,
                            will_be_spoken: true,
                        },
                    )));
                } else {
                    frames.push(Frame::new(FrameData::LlmText(TextFrame::llm(delta))));
                }
            }
            Event::Transcript {
                item_id,
                text,
                is_final,
            } => {
                let transcript = TranscriptionFrame {
                    text: text.clone(),
                    finalized: *is_final,
                    ..Default::default()
                };
                let mut frame = Frame::new(if *is_final {
                    FrameData::Transcription(transcript)
                } else {
                    FrameData::InterimTranscription(transcript)
                })
                .with_interruptible(!*is_final);
                frame
                    .metadata
                    .insert("transcript_id".into(), json!(item_id));
                frame.metadata.insert("item_id".into(), json!(item_id));
                return Ok(vec![(frame, Direction::Upstream)]);
            }
            Event::ToolCall { response_id, call } => {
                match &self.active {
                    Some(active) if active != response_id => {
                        return Err(fatal("Voice Live overlapping tool response lifecycle"));
                    }
                    Some(_) => {}
                    None => {
                        // Native assistant tool admission marks its LLM active.
                        // A real provider tool response therefore needs a balanced
                        // LLM-only lifecycle so its native continuation can run.
                        self.active = Some(response_id.clone());
                        frames.push(Frame::new(FrameData::LlmResponseStart));
                    }
                }
                frames.push(Frame::new(FrameData::FunctionCallsStarted(vec![
                    call.clone()
                ])))
            }
            Event::ResponseDone {
                response_id,
                status,
                usage,
            } => {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active != response_id)
                {
                    return Err(fatal(
                        "Voice Live completion conflicts with its active response",
                    ));
                }
                for mut frame in self.close() {
                    if *status == crate::ResponseStatus::Failed
                        && matches!(frame.data, FrameData::LlmResponseEnd)
                    {
                        frame.metadata.insert("failed".into(), json!(true));
                    }
                    frames.push(frame);
                }
                if usage_metrics {
                    if let Some(usage) = usage {
                        frames.push(Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
                            processor: "AzureVoiceLiveService".into(),
                            prompt_tokens: usage.input_tokens.unwrap_or(0),
                            completion_tokens: usage.output_tokens.unwrap_or(0),
                            total_tokens: usage.total_tokens.unwrap_or(0),
                        }])));
                    }
                }
            }
            Event::SpeechStarted if !manual => {
                return Ok(vec![(
                    Frame::new(FrameData::ProposedUserStartedSpeaking),
                    Direction::Upstream,
                )])
            }
            Event::SpeechStopped if !manual => {
                return Ok(vec![(
                    Frame::new(FrameData::ProposedUserStoppedSpeaking),
                    Direction::Upstream,
                )])
            }
            Event::ProviderError {
                message,
                fatal: true,
                ..
            } => return Err(ProcessorError::fatal(message.clone())),
            Event::ProviderError { message, .. } => {
                frames.push(Frame::new(FrameData::Error(ErrorFrame::new(message))))
            }
            Event::TranscriptFailed { .. } => {}
            _ => {}
        }
        for frame in &mut frames {
            frame
                .metadata
                .insert("voice_live_generation".into(), json!(event.generation));
            if let Some(id) = response_id(&event.event) {
                frame.metadata.insert("response_id".into(), json!(id));
            }
            if let Event::Audio {
                item_id,
                content_index,
                ..
            } = &event.event
            {
                frame.metadata.insert("item_id".into(), json!(item_id));
                frame
                    .metadata
                    .insert("content_index".into(), json!(content_index));
            }
        }
        Ok(frames
            .into_iter()
            .map(|frame| (frame, Direction::Downstream))
            .collect())
    }
}
fn response_id(event: &Event) -> Option<&str> {
    match event {
        Event::ResponseStarted { response_id }
        | Event::ResponseDone { response_id, .. }
        | Event::Audio { response_id, .. }
        | Event::Text { response_id, .. }
        | Event::ToolCall { response_id, .. } => Some(response_id),
        _ => None,
    }
}

fn admit_event_mapping<T>(
    sender: &VoiceLiveSender,
    event: &VoiceLiveEvent,
    map: impl FnOnce() -> T,
) -> Option<T> {
    if response_id(&event.event).is_some() {
        sender.with_current_generation(event.generation, map)
    } else {
        // Input transcription and provider errors do not belong to a cancelled
        // assistant response generation.
        Some(map())
    }
}

/// A native input boundary must be consumed before its socket actor resumes.
/// The ordered normal flush waits for preceding system processing at every
/// upstream hop, bounces at the actual ingress, then follows any emitted native
/// interruption back to this provider. Neither public origin text nor a caller
/// supplied flush event is acknowledgement authority.
fn semantic_admission_frames(
    event: &Event,
    barrier: pipecat_core::LocalFlushEvent,
) -> Result<[Frame; 2], ProcessorError> {
    let proposal = match event {
        Event::SpeechStarted => FrameData::ProposedUserStartedSpeaking,
        Event::SpeechStopped => FrameData::ProposedUserStoppedSpeaking,
        _ => {
            return Err(fatal(
                "Voice Live semantic admission requires a speech boundary",
            ))
        }
    };
    Ok([
        Frame::new(proposal).with_interruptible(false),
        Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
            event: Some(barrier),
            returning: false,
            origin: Some("voice-live-semantic-admission".into()),
        }))
        .with_interruptible(false),
    ])
}

/// Single-session fixed-profile provider. The application uses a restricted
/// handle rather than exposing this processor's immutable low-level client.
pub struct VoiceLiveProcessor {
    client: VoiceLiveClient,
    sender: Arc<Mutex<Option<VoiceLiveSender>>>,
    history: Arc<Mutex<History>>,
    mapper: Arc<Mutex<Mapper>>,
    admission: Mutex<RealtimeContextAdmission>,
    assistant_admission: Mutex<RealtimeContextAdmission>,
    initial: Mutex<Option<ConversationItem>>,
    owner: Mutex<Option<Arc<tokio::sync::Mutex<VoiceLiveSession>>>>,
    joined: Mutex<Option<Arc<AtomicBool>>>,
    started: AtomicBool,
    input_pending: AtomicBool,
    terminal: Arc<AtomicBool>,
    draining: AtomicBool,
    result: Arc<Mutex<Option<Result<(), ProcessorError>>>>,
    receive_ready: Arc<tokio::sync::Notify>,
    completed: Arc<AtomicU64>,
}
impl VoiceLiveProcessor {
    /// Standalone processors cannot offer local tools or semantic VAD: their
    /// custom topology cannot prove native executor entry or user admission.
    /// Use `VoiceLiveApp` for the guarded real native composition.
    pub fn new(client: VoiceLiveClient) -> Result<Self, VoiceLiveError> {
        if !client.config().session.tools.is_empty() || !client.config().session.is_manual() {
            return Err(VoiceLiveError::Invalid(
                "local tools and semantic VAD require the owned VoiceLiveApp topology",
            ));
        }
        Self::new_app(client)
    }
    pub(crate) fn new_app(client: VoiceLiveClient) -> Result<Self, VoiceLiveError> {
        client.config().validate()?;
        let history = History {
            max_messages: client.config().limits.max_history_messages.min(MAX_HISTORY),
            max_transcripts: client
                .config()
                .limits
                .max_session_items
                .min(MAX_TRANSCRIPTS),
            ..History::default()
        };
        Ok(Self {
            client: client
                .require_native_receipts()
                .with_native_semantic_admission(),
            sender: Arc::new(Mutex::new(None)),
            history: Arc::new(Mutex::new(history)),
            mapper: Arc::new(Mutex::new(Mapper::default())),
            admission: Mutex::new(RealtimeContextAdmission::default()),
            assistant_admission: Mutex::new(RealtimeContextAdmission::default()),
            initial: Mutex::new(None),
            owner: Mutex::new(None),
            joined: Mutex::new(None),
            started: AtomicBool::new(false),
            input_pending: AtomicBool::new(false),
            terminal: Arc::new(AtomicBool::new(false)),
            draining: AtomicBool::new(false),
            result: Arc::new(Mutex::new(None)),
            receive_ready: Arc::new(tokio::sync::Notify::new()),
            completed: Arc::new(AtomicU64::new(0)),
        })
    }
    pub(crate) fn with_initial_context(
        self,
        snapshot: LlmContextData,
        item: Option<ConversationItem>,
    ) -> Result<Self, ProcessorError> {
        bounded(&snapshot, self.client.config().limits.max_message_bytes)?;
        for message in &snapshot.messages {
            plain(message)?;
            if message.role == ChatRole::Tool
                || !message.tool_calls.is_empty()
                || message.tool_call_id.is_some()
            {
                return Err(fatal(
                    "Voice Live initial context cannot import tool authority",
                ));
            }
        }
        self.history
            .lock()
            .map_err(|_| fatal("Voice Live history lock"))?
            .accepted = snapshot.messages;
        self.history
            .lock()
            .map_err(|_| fatal("Voice Live history lock"))?
            .bound(self.client.config().limits.max_message_bytes)?;
        *self
            .initial
            .lock()
            .map_err(|_| fatal("Voice Live seed lock"))? = item;
        Ok(self)
    }
    fn sender(&self) -> Result<VoiceLiveSender, ProcessorError> {
        self.sender
            .lock()
            .map_err(|_| fatal("Voice Live sender lock"))?
            .clone()
            .ok_or_else(|| fatal("Voice Live session is not connected"))
    }
    pub fn acknowledge_playback(&self, position: PlaybackPosition) -> Result<(), ProcessorError> {
        self.sender()?.acknowledge_playback(position).map_err(error)
    }
    pub fn session_joined(&self) -> bool {
        self.joined
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|joined| joined.load(Ordering::Acquire))
    }
    pub fn responses_completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }
    pub fn tool_result_relay(&self) -> VoiceLiveToolResultRelay {
        VoiceLiveToolResultRelay {
            sender: self.sender.clone(),
            history: self.history.clone(),
            limit: self.client.config().limits.max_message_bytes,
        }
    }
    fn context(
        &self,
        data: &LlmContextData,
        cause: Option<Arc<NativeRealtimeContextCause>>,
        state_only: bool,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        let assistant = cause
            .as_ref()
            .is_some_and(|cause| cause.intent() == RealtimeContextIntent::AssistantContinuation);
        let mut admission = if assistant {
            &self.assistant_admission
        } else {
            &self.admission
        }
        .lock()
        .map_err(|_| fatal("Voice Live context admission lock"))?;
        if let Some(cause) = &cause {
            cause.validate_direction(direction)?;
            if !admission.check(cause)? {
                return Ok(());
            }
        } else if !state_only {
            return Err(fatal(
                "Voice Live live contexts require native paired intent",
            ));
        }
        let mut state = self
            .history
            .lock()
            .map_err(|_| fatal("Voice Live history lock"))?;
        let mut next = state.clone();
        let append = match cause.as_ref().map(|cause| cause.intent()) {
            Some(RealtimeContextIntent::Replace) => {
                return Err(fatal(
                    "Voice Live history replacement requires a new session",
                ))
            }
            Some(RealtimeContextIntent::Append { start, end }) => Some((start, end)),
            _ => None,
        };
        if assistant {
            let cause = cause.as_ref().expect("assistant cause");
            if !cause.assistant_native_progress_reasons().is_empty()
                || cause
                    .assistant_selected_native_terminal_route_id()
                    .is_some()
            {
                return Err(fatal(
                    "Voice Live supports only final native tool continuation",
                ));
            }
            if cause
                .assistant_native_outcome_id()
                .is_none_or(|id| !next.outcomes.contains(&id))
            {
                return Err(fatal(
                    "Voice Live assistant continuation lacks its admitted native terminal",
                ));
            }
        }
        let mut actions = next.snapshot(
            data,
            append,
            state_only,
            self.client.config().limits.max_message_bytes,
        )?;
        let run = cause.as_ref().is_some_and(|cause| cause.run_llm());
        if self.draining.load(Ordering::Acquire) {
            if !actions.is_empty() {
                return Err(fatal("Voice Live drain cannot accept new history"));
            }
        } else if run {
            actions.push(Action::CreateResponse);
        }
        if !actions.is_empty() {
            let fence = cause
                .as_ref()
                .and_then(|cause| cause.inference_fence())
                .cloned()
                .ok_or_else(|| fatal("Voice Live explicit context lacks invocation fence"))?;
            self.sender()?
                .try_send_batch_with_fence(actions, Some(fence))
                .map_err(error)?;
        }
        *state = next;
        if let Some(cause) = cause {
            admission.commit(cause);
        }
        Ok(())
    }
    async fn connect(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let session = self.client.connect().await.map_err(error)?;
        let sender = session.sender();
        let joined = session.join_observer();
        let session = Arc::new(tokio::sync::Mutex::new(session));
        // Publish the real recovery owner before any subsequent await.
        *self.owner.lock().unwrap_or_else(|error| error.into_inner()) = Some(session.clone());
        *self
            .joined
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(joined);
        *self
            .sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender.clone());
        let initial = self
            .initial
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(item) = initial {
            if let Err(primary) = sender.try_send(Action::CreateItem(item)) {
                return merge(
                    Err(error(primary)),
                    session.lock().await.shutdown().await.map_err(error),
                );
            }
        }
        let history = self.history.clone();
        let mapper = self.mapper.clone();
        let ready = self.receive_ready.clone();
        let result_slot = self.result.clone();
        let terminal = self.terminal.clone();
        let completed = self.completed.clone();
        let limit = self.client.config().limits.max_message_bytes;
        let manual = self.client.config().session.is_manual();
        let usage = config.enable_usage_metrics;
        let output = ctx.clone();
        let (handoff, receive) =
            tokio::sync::oneshot::channel::<Arc<tokio::sync::Mutex<VoiceLiveSession>>>();
        let (owned, ownership) = tokio::sync::oneshot::channel::<()>();
        let task = ctx.spawn("azure-voice-live-receive", false, async move {
            let Ok(owner) = receive.await else {
                return Ok(());
            };
            let mut session = owner.lock().await;
            let _ = owned.send(());
            let primary = async {
                ready.notified().await;
                while let Some(received) = session.next_event().await {
                    let event = received.map_err(error)?;
                    if !manual && matches!(event.event, Event::SpeechStarted | Event::SpeechStopped)
                    {
                        let Some(barrier) = sender.semantic_admission_barrier().map_err(error)?
                        else {
                            if terminal.load(Ordering::Acquire) {
                                // Finish abandons, rather than acknowledges, a
                                // boundary whose normal-lane return may be held
                                // behind the private End gate.
                                continue;
                            }
                            return Err(fatal("Voice Live native speech admission is missing"));
                        };
                        for frame in semantic_admission_frames(&event.event, barrier)? {
                            // Input boundaries are not response output. An unrelated
                            // interruption must not drop the proposal while allowing
                            // its durable barrier to certify nonexistent consumption.
                            output.push(frame, Direction::Upstream).await?;
                        }
                        continue;
                    }
                    let response = response_id(&event.event).map(str::to_owned);
                    let fence = response.as_deref().and_then(|id| sender.response_fence(id));
                    let map = || {
                        let mut history = history
                            .lock()
                            .map_err(|_| fatal("Voice Live history lock"))?;
                        if !history.observe(&event.event, limit)? {
                            return Ok(vec![]);
                        }
                        let mut mapper =
                            mapper.lock().map_err(|_| fatal("Voice Live mapper lock"))?;
                        let frames = mapper.frames(&event, usage, manual)?;
                        if matches!(event.event, Event::ResponseDone { .. }) {
                            completed
                                .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                                    value.checked_add(1)
                                })
                                .map_err(|_| fatal("Voice Live response counter overflow"))?;
                        }
                        Ok::<_, ProcessorError>(frames)
                    };
                    let mapped = admit_event_mapping(&sender, &event, map).transpose()?;
                    let Some(frames) = mapped else {
                        if let Event::ToolCall { call, .. } = &event.event {
                            sender
                                .discard_unmapped_tool(call, event.generation)
                                .map_err(error)?;
                        }
                        continue;
                    };
                    if frames.is_empty() {
                        if let Event::ToolCall { call, .. } = &event.event {
                            sender
                                .discard_unmapped_tool(call, event.generation)
                                .map_err(error)?;
                        }
                    }
                    for (frame, direction) in frames {
                        if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
                            // The weak native identity is registered before any
                            // queue owns the declaration. If fencing drops it,
                            // expiration proves non-entry; it is not settlement.
                            sender
                                .register_tool_declaration(&frame, event.generation)
                                .map_err(error)?;
                        }
                        if let FrameData::Error(problem) = &frame.data {
                            output.push_error(problem.clone()).await?;
                            continue;
                        }
                        if response.is_some() {
                            let sent = sender
                                .while_current(event.generation, async {
                                    if let Some(fence) = &fence {
                                        output
                                            .try_push_with_invocation_fence(frame, direction, fence)
                                    } else {
                                        output.try_push(frame, direction)
                                    }
                                })
                                .await;
                            if let Some(result) = sent {
                                result?;
                            }
                        } else {
                            output.push(frame, direction).await?;
                        }
                    }
                }
                if !terminal.load(Ordering::Acquire) {
                    return Err(fatal(
                        "Voice Live provider ended before intentional shutdown",
                    ));
                }
                Ok(())
            }
            .await;
            let result = merge(primary, session.shutdown().await.map_err(error));
            publish(&result_slot, result.clone());
            result
        });
        match task {
            Ok(_) => {
                if let Err(session) = handoff.send(session) {
                    return merge(
                        Err(fatal("Voice Live receive owner closed before handoff")),
                        session.lock().await.shutdown().await.map_err(error),
                    );
                }
                ownership
                    .await
                    .map_err(|_| fatal("Voice Live receive ownership handoff failed"))
            }
            Err(primary) => merge(
                Err(primary),
                session.lock().await.shutdown().await.map_err(error),
            ),
        }
    }
    async fn join_owner(&self) -> Result<(), ProcessorError> {
        let owner = self
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(owner) = owner else {
            return Ok(());
        };
        let result = owner.lock().await.shutdown().await.map_err(error);
        self.owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        result
    }
    fn close(&self) {
        self.terminal.store(true, Ordering::Release);
        if let Some(sender) = self
            .sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            sender.close();
        }
    }
    /// Runs in the private application gate, not the lane that receives tools.
    pub(crate) async fn drain_final_tools(&self) -> Result<(), ProcessorError> {
        let limits = &self.client.config().limits;
        let budget = limits
            .response_timeout
            .checked_mul(2)
            .and_then(|value| value.checked_add(limits.operation_timeout))
            .and_then(|value| value.checked_add(limits.close_timeout))
            .ok_or_else(|| fatal("Voice Live drain budget overflow"))?;
        self.terminal.store(true, Ordering::Release);
        let first = !self.draining.swap(true, Ordering::AcqRel);
        let sender = self.sender()?;
        let drain = async {
            if first {
                if let Err(primary) = sender.finish().await {
                    publish(&self.result, Err(error(primary)));
                    sender.close();
                }
            }
            // Finish acceptance is not shutdown proof. This watch is published
            // only after the supervisor has joined the actual socket actor.
            let finished = sender.wait_finished().await.map_err(error);
            let joined = self.join_owner().await;
            let received = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
                .unwrap_or_else(|| Err(fatal("Voice Live receive owner has no terminal result")));
            merge(merge(received, finished), joined)
        };
        match tokio::time::timeout(budget, drain).await {
            Ok(result) => result,
            Err(_) => {
                sender.close();
                let failure = fatal("Voice Live final-tool drain timed out");
                publish(&self.result, Err(failure.clone()));
                Err(failure)
            }
        }
    }
    pub(crate) fn enter_tool_executor(&self, frame: &Frame) -> Result<bool, ProcessorError> {
        self.sender()?.enter_tool_executor(frame).map_err(error)
    }
    #[cfg(test)]
    pub(crate) fn install_test_sender(&self, sender: VoiceLiveSender) {
        *self.sender.lock().unwrap() = Some(sender);
    }
}
fn publish(slot: &Mutex<Option<Result<(), ProcessorError>>>, result: Result<(), ProcessorError>) {
    let mut slot = slot.lock().unwrap_or_else(|error| error.into_inner());
    if slot.is_none() || slot.as_ref().is_some_and(Result::is_ok) {
        *slot = Some(result);
    }
}
fn merge(
    primary: Result<(), ProcessorError>,
    cleanup: Result<(), ProcessorError>,
) -> Result<(), ProcessorError> {
    match (primary, cleanup) {
        (Ok(()), result) | (result, Ok(())) => result,
        (Err(primary), Err(cleanup)) => Err(ProcessorError::fatal(format!(
            "{}; Voice Live cleanup: {}",
            primary.message, cleanup.message
        ))),
    }
}
impl Drop for VoiceLiveProcessor {
    fn drop(&mut self) {
        self.close();
    }
}
#[async_trait]
impl FrameProcessor for VoiceLiveProcessor {
    fn name(&self) -> &str {
        "AzureVoiceLiveService"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(fatal("Voice Live processor is single-session"));
        }
        if config.audio_in_sample_rate != 24_000 || config.audio_out_sample_rate != 24_000 {
            return Err(fatal(
                "Voice Live pipeline requires 24000 Hz input and output",
            ));
        }
        self.connect(config, ctx).await
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch()?;
        frame.native_tool_outcome()?;
        frame.native_tool_progress_admission()?;
        frame.native_tool_progress()?;
        if frame.has_native_tool_progress_restriction()
            || frame
                .native_llm_continuation()
                .map_err(|_| fatal("invalid native Voice Live continuation"))?
                .is_some()
        {
            return Err(fatal("Voice Live supports final native tools only"));
        }
        if direction == Direction::Downstream {
            if let FrameData::FlushDetailed(flush) = &frame.data {
                if flush.returning {
                    if let Some(barrier) = &flush.event {
                        if self
                            .sender()?
                            .acknowledge_semantic_barrier(barrier)
                            .map_err(error)?
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
        let cause = frame.take_native_realtime_context_cause()?;
        if let Some(cause) = &cause {
            cause.validate_direction(direction)?;
        }
        if matches!(
            frame.data,
            FrameData::Start(_) | FrameData::StartDetailed(_)
        ) {
            ctx.push(frame, direction).await?;
            let external = pipecat_core::UserTurnStrategy::External {
                enable_interruptions: true,
            };
            ctx.broadcast(Frame::new(FrameData::LlmServiceMetadata(
                pipecat_core::LlmServiceMetadataFrame {
                    base: pipecat_core::ServiceMetadataFrame {
                        service_name: self.name().into(),
                        user_turn_strategies: (!self.client.config().session.is_manual()).then(
                            || pipecat_core::UserTurnStrategies {
                                start: Some(vec![external.clone()]),
                                stop: Some(vec![external]),
                            },
                        ),
                    },
                    is_realtime_service: true,
                },
            )))
            .await?;
            self.receive_ready.notify_one();
            return Ok(());
        }
        match &frame.data {
            FrameData::LlmContext(context) => {
                self.context(
                    &context.clone().into(),
                    cause,
                    frame.is_realtime_context_update(),
                    direction,
                )?;
                return Ok(());
            }
            FrameData::LlmContextDetailed(context) => {
                if context.speculation {
                    return Err(fatal("Voice Live does not support speculative context"));
                }
                self.context(
                    &context.context,
                    cause,
                    frame.is_realtime_context_update(),
                    direction,
                )?;
                return Ok(());
            }
            FrameData::InputAudio(_) | FrameData::UserAudio(_)
                if direction == Direction::Downstream =>
            {
                let audio = match &frame.data {
                    FrameData::InputAudio(audio) => audio,
                    FrameData::UserAudio(audio) => &audio.audio,
                    _ => unreachable!("audio family matched above"),
                };
                audio
                    .validate()
                    .map_err(|_| fatal("Voice Live input requires aligned PCM16"))?;
                if audio.sample_rate != 24_000
                    || audio.num_channels != 1
                    || audio.audio.len() > self.client.config().limits.max_audio_chunk_bytes
                    || audio.audio.len() % 2 != 0
                {
                    return Err(fatal(
                        "Voice Live input requires bounded 24000 Hz mono PCM16",
                    ));
                }
                if !audio.audio.is_empty() {
                    self.sender()?
                        .try_send(Action::AppendAudio(audio.audio.clone()))
                        .map_err(error)?;
                    self.input_pending.store(true, Ordering::Release);
                }
            }
            FrameData::UserStoppedSpeaking
                if direction == Direction::Downstream
                    && self.client.config().session.is_manual() =>
            {
                if self.input_pending.swap(false, Ordering::AcqRel) {
                    self.sender()?
                        .try_send_batch_with_fence(
                            vec![Action::CommitAudio, Action::CreateResponse],
                            Some(ctx.invocation_fence()?),
                        )
                        .map_err(error)?;
                }
            }
            FrameData::SpeechControlParams(params) if self.client.config().session.is_manual() => {
                if let Some(vad) = &params.vad_params {
                    let seconds = vad.start_secs + 0.3;
                    if !seconds.is_finite()
                        || vad.start_secs < 0.0
                        || seconds * 1000.0 > f64::from(self.client.config().limits.max_preroll_ms)
                    {
                        return Err(fatal("Voice Live speech-control preroll exceeds bound"));
                    }
                    let samples = (seconds * 24_000.0).ceil() as usize;
                    self.sender()?
                        .try_send(Action::SetPrerollBytes(samples * 2))
                        .map_err(error)?;
                }
            }
            FrameData::Interruption => {
                self.sender()?.interrupt(None);
                let closed = self
                    .mapper
                    .lock()
                    .map_err(|_| fatal("Voice Live mapper lock"))?
                    .close();
                ctx.push(frame, direction).await?;
                for frame in closed {
                    ctx.push(frame, Direction::Downstream).await?;
                }
                return Ok(());
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.close(),
            FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_) => {
                return Err(fatal(
                    "Voice Live terminal frames must enter through the native terminal relay",
                ));
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.drain_final_tools().await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close();
        self.join_owner().await
    }
}

/// Validates the unmodified native carrier before both intended consumers. It
/// admits the provider command before forwarding the same receipt to assistant
/// history/scheduling, without waiting for a local journal as delivery proof.
pub struct VoiceLiveToolResultRelay {
    sender: Arc<Mutex<Option<VoiceLiveSender>>>,
    history: Arc<Mutex<History>>,
    limit: usize,
}
#[async_trait]
impl FrameProcessor for VoiceLiveToolResultRelay {
    fn name(&self) -> &str {
        "VoiceLiveToolResultRelay"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch()?;
        let outcome = frame.native_tool_outcome()?;
        if frame.native_tool_progress()?.is_some()
            || frame.native_tool_progress_admission()?.is_some()
            || frame.has_native_tool_progress_restriction()
        {
            return Err(fatal(
                "Voice Live does not support progressive tool envelopes",
            ));
        }
        if direction == Direction::Downstream
            && (matches!(
                frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
            ) || matches!(frame.data, FrameData::FunctionCallCancel(_)) && outcome.is_some())
        {
            {
                let mut history = self
                    .history
                    .lock()
                    .map_err(|_| fatal("Voice Live history lock"))?;
                let mut next = history.clone();
                if next.terminal(&frame, self.limit)? {
                    self.sender
                        .lock()
                        .map_err(|_| fatal("Voice Live sender lock"))?
                        .as_ref()
                        .ok_or_else(|| fatal("Voice Live terminal relay has no provider"))?
                        .try_send_native_result(&frame)
                        .map_err(error)?;
                    *history = next;
                }
            }
        }
        ctx.push(frame, direction).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResponseStatus;
    use pipecat_core::{ChatMessage, ToolSet};
    use std::time::Duration;

    struct HeldNativeUser {
        inner: Arc<dyn FrameProcessor>,
        held: AtomicBool,
        arrived: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl FrameProcessor for HeldNativeUser {
        fn name(&self) -> &str {
            self.inner.name()
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.inner.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::ProposedUserStartedSpeaking)
                && !self.held.swap(true, Ordering::AcqRel)
            {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            self.inner.process(frame, direction, ctx).await
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.inner.prepare_end(ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.inner.cleanup().await
        }
    }
    struct SemanticOrderingProbe {
        barrier: Arc<Mutex<pipecat_core::LocalFlushEvent>>,
        interruptions: Arc<AtomicU64>,
        interruption_arrived: Arc<tokio::sync::Notify>,
        release_interruption: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl FrameProcessor for SemanticOrderingProbe {
        fn name(&self) -> &str {
            "SemanticOrderingProbe"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(&frame.data, FrameData::InputTransportMessage(value) if value == "start") {
                let barrier = self.barrier.lock().unwrap().clone();
                for frame in semantic_admission_frames(&Event::SpeechStarted, barrier)? {
                    ctx.push(frame, Direction::Upstream).await?;
                }
                return Ok(());
            }
            if matches!(frame.data, FrameData::Interruption) {
                self.interruption_arrived.notify_one();
                self.release_interruption.notified().await;
                self.interruptions.fetch_add(1, Ordering::AcqRel);
            }
            if direction == Direction::Downstream {
                if let FrameData::FlushDetailed(flush) = &frame.data {
                    let barrier = self.barrier.lock().unwrap().clone();
                    if flush.returning && flush.event.as_ref() == Some(&barrier) {
                        barrier.set();
                        return Ok(());
                    }
                }
            }
            ctx.push(frame, direction).await
        }
    }

    #[tokio::test]
    async fn semantic_roundtrip_waits_for_real_user_and_provider_system_processing() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let pair = pipecat_turns::LLMContextAggregatorPair::with_realtime_mode(
                pipecat_services::Context::default(),
                pipecat_turns::UserAggregatorConfig {
                    turn: pipecat_turns::UserTurnConfig {
                        resolve_proposals: true,
                        ..Default::default()
                    },
                    empty_user_turn: None,
                    ..Default::default()
                },
                Default::default(),
                pipecat_turns::RealtimeServiceMode::Enabled,
            )
            .unwrap();
            let first = pipecat_core::LocalFlushEvent::default();
            let barrier = Arc::new(Mutex::new(first.clone()));
            let user_arrived = Arc::new(tokio::sync::Notify::new());
            let release_user = Arc::new(tokio::sync::Notify::new());
            let interruption_arrived = Arc::new(tokio::sync::Notify::new());
            let release_interruption = Arc::new(tokio::sync::Notify::new());
            let interruptions = Arc::new(AtomicU64::new(0));
            let worker = pipecat_core::Pipeline::new(vec![
                Arc::new(pipecat_core::IdentityProcessor),
                Arc::new(HeldNativeUser {
                    inner: pair.user(),
                    held: AtomicBool::new(false),
                    arrived: user_arrived.clone(),
                    release: release_user.clone(),
                }),
                Arc::new(pipecat_core::IdentityProcessor),
                Arc::new(SemanticOrderingProbe {
                    barrier: barrier.clone(),
                    interruptions: interruptions.clone(),
                    interruption_arrived: interruption_arrived.clone(),
                    release_interruption: release_interruption.clone(),
                }),
                pair.assistant(),
            ])
            .start(Default::default())
            .await
            .unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::InputTransportMessage(json!("start"))),
                    Direction::Upstream,
                )
                .await
                .unwrap();
            user_arrived.notified().await;
            assert!(!first.is_set(), "queued proposal is not native consumption");
            release_user.notify_one();
            interruption_arrived.notified().await;
            assert!(
                !first.is_set(),
                "queued interruption is not provider consumption"
            );
            release_interruption.notify_one();
            first.wait().await;
            assert_eq!(interruptions.load(Ordering::Acquire), 1);

            // The native controller is already speaking. A repeated real start
            // is a no-op, yet its exact barrier must still return without an
            // invented interruption or a skip-next-generation heuristic.
            let duplicate = pipecat_core::LocalFlushEvent::default();
            *barrier.lock().unwrap() = duplicate.clone();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::InputTransportMessage(json!("start"))),
                    Direction::Upstream,
                )
                .await
                .unwrap();
            duplicate.wait().await;
            assert_eq!(interruptions.load(Ordering::Acquire), 1);
            worker.end().await.unwrap();
            let (output, report) = worker.collect().await.unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(output.iter().all(|item| {
                !matches!(&item.frame.data, FrameData::FlushDetailed(flush)
                    if flush.event.as_ref() == Some(&first)
                        || flush.event.as_ref() == Some(&duplicate))
            }));
        })
        .await
        .expect("native semantic admission ordering did not terminate");
    }

    #[test]
    fn semantic_boundaries_and_markers_are_durable_exact_local_frames() {
        for event in [Event::SpeechStarted, Event::SpeechStopped] {
            let barrier = pipecat_core::LocalFlushEvent::default();
            let [proposal, marker] = semantic_admission_frames(&event, barrier.clone()).unwrap();
            assert!(!proposal.interruptible);
            assert!(!marker.interruptible);
            assert!(matches!(&marker.data, FrameData::FlushDetailed(flush)
                if !flush.returning && flush.event.as_ref() == Some(&barrier)));
            assert!(matches!(
                (&event, &proposal.data),
                (Event::SpeechStarted, FrameData::ProposedUserStartedSpeaking)
                    | (Event::SpeechStopped, FrameData::ProposedUserStoppedSpeaking)
            ));
        }
    }
    #[test]
    fn final_input_transcript_is_durable_but_interim_remains_interruptible() {
        for is_final in [false, true] {
            let frames = Mapper::default()
                .frames(
                    &event(Event::Transcript {
                        item_id: "input-item".into(),
                        text: "spoken".into(),
                        is_final,
                    }),
                    false,
                    false,
                )
                .unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].1, Direction::Upstream);
            assert_eq!(frames[0].0.interruptible, !is_final);
            assert_eq!(
                frames[0].0.metadata.get("transcript_id"),
                Some(&json!("input-item"))
            );
        }
    }
    #[test]
    fn old_response_generation_cannot_discard_completed_input_transcription() {
        let (sender, _, generation) = crate::client::delivery_tests::test_sender();
        generation.store(1, Ordering::Release);
        let transcript = event(Event::Transcript {
            item_id: "delayed-input".into(),
            text: "kept".into(),
            is_final: true,
        });
        let frames = admit_event_mapping(&sender, &transcript, || {
            Mapper::default().frames(&transcript, false, false)
        })
        .expect("input is not response-generation-owned")
        .unwrap();
        assert_eq!(frames.len(), 1);
        assert!(!frames[0].0.interruptible);
        assert!(matches!(&frames[0].0.data, FrameData::Transcription(text) if text.text == "kept"));
        let response = event(Event::Text {
            response_id: "old-response".into(),
            item_id: "old-output".into(),
            delta: "discard".into(),
            audio_transcript: false,
        });
        let mut mapped = false;
        assert!(admit_event_mapping(&sender, &response, || mapped = true).is_none());
        assert!(!mapped, "stale generated output must never be mapped");
    }
    #[test]
    fn standalone_processor_rejects_semantic_profile_without_owned_user_route() {
        let mut config = crate::VoiceLiveConfig::new(
            "ws://127.0.0.1:1/voice-live/realtime",
            "synthetic",
            crate::VoiceLiveCredentials::ApiKey("invented".into()),
            "synthetic-voice",
        );
        config.session.turn_detection = crate::TurnDetection::AzureSemanticVad(Default::default());
        assert!(VoiceLiveProcessor::new(VoiceLiveClient::new(config).unwrap()).is_err());
    }
    fn message(role: ChatRole, text: &str) -> ContextMessage {
        ChatMessage::new(role, text).into()
    }
    fn snapshot(messages: Vec<ContextMessage>) -> LlmContextData {
        LlmContextData {
            messages,
            tools: ToolSet::Functions(vec![]),
            tool_choice: None,
        }
    }
    fn event(event: Event) -> VoiceLiveEvent {
        VoiceLiveEvent {
            event,
            generation: 0,
        }
    }
    #[test]
    fn tool_bearing_processor_requires_the_owned_guarded_application() {
        let mut config = crate::VoiceLiveConfig::new(
            "wss://synthetic.invalid/voice-live/realtime",
            "synthetic-model",
            crate::VoiceLiveCredentials::ApiKey("invented-key".into()),
            "synthetic-voice",
        );
        let schema = crate::FunctionTool::new("lookup", "synthetic", json!({"type":"object"}));
        config.session.tools.push(schema.clone());
        assert!(matches!(
            VoiceLiveProcessor::new(VoiceLiveClient::new(config.clone()).unwrap()),
            Err(VoiceLiveError::Invalid(_))
        ));
        let executions = Arc::new(AtomicU64::new(0));
        let count = executions.clone();
        let handler = pipecat_processors::tools::RegisteredTool::new(Arc::new(move |_| {
            let count = count.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"synthetic":true}))
            })
        }));
        assert!(crate::VoiceLiveApp::new(
            config,
            pipecat_services::Context::default(),
            vec![crate::VoiceLiveLocalTool { schema, handler }],
            Arc::new(pipecat_core::IdentityProcessor)
        )
        .is_ok());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            0,
            "construction has no callback side effect"
        );
    }

    #[test]
    fn delayed_item_claim_survives_tool_history_and_blank_repeat_then_typed_same_text() {
        let mut history = History::default();
        let call = FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "call-1".into(),
            arguments: json!({}),
        };
        history
            .observe(
                &Event::ToolCall {
                    response_id: "r1".into(),
                    call: call.clone(),
                },
                65536,
            )
            .unwrap();
        history
            .observe(
                &Event::Transcript {
                    item_id: "voice-1".into(),
                    text: "same words".into(),
                    is_final: true,
                },
                65536,
            )
            .unwrap();
        assert!(!history
            .observe(
                &Event::Transcript {
                    item_id: "voice-1".into(),
                    text: "   ".into(),
                    is_final: true
                },
                65536
            )
            .unwrap());
        let mut assistant = message(ChatRole::Assistant, "");
        assistant.tool_calls.push(call);
        let mut tool = message(ChatRole::Tool, "IN_PROGRESS");
        tool.tool_call_id = Some("call-1".into());
        let mut messages = vec![assistant, tool];
        assert!(history
            .snapshot(&snapshot(messages.clone()), None, true, 65536)
            .unwrap()
            .is_empty());
        assert_eq!(
            history.transcripts.get("voice-1").map(String::as_str),
            Some("same words")
        );
        messages.push(message(ChatRole::User, "same words"));
        assert!(history
            .snapshot(&snapshot(messages.clone()), None, true, 65536)
            .unwrap()
            .is_empty());
        assert!(history.transcripts.contains_key("voice-1"));
        messages.push(message(ChatRole::User, "same words"));
        let actions = history
            .snapshot(&snapshot(messages), Some((3, 4)), false, 65536)
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0],Action::CreateItem(item) if item.content.as_ref().unwrap()[0].text.as_deref()==Some("same words"))
        );
    }
    #[test]
    fn explicit_typed_range_never_consumes_a_pending_same_text_transcript() {
        let mut history = History::default();
        history
            .observe(
                &Event::Transcript {
                    item_id: "spoken".into(),
                    text: "repeat".into(),
                    is_final: true,
                },
                65536,
            )
            .unwrap();
        let messages = vec![message(ChatRole::User, "repeat")];
        assert_eq!(
            history
                .snapshot(&snapshot(messages), Some((0, 1)), false, 65536)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            history.transcripts.get("spoken").map(String::as_str),
            Some("repeat")
        );
    }
    #[test]
    fn explicit_append_survives_a_prior_trailing_observation_of_the_same_message() {
        let mut history = History::default();
        let messages = vec![message(ChatRole::User, "same")];
        assert!(history
            .snapshot(&snapshot(messages.clone()), None, true, 65536)
            .unwrap()
            .is_empty());
        let actions = history
            .snapshot(&snapshot(messages), Some((0, 1)), false, 65536)
            .unwrap();
        assert_eq!(actions.len(), 1);
    }
    #[test]
    fn completed_transcript_identity_rejects_conflict_and_history_mutation_fails_closed() {
        let mut history = History::default();
        history
            .observe(
                &Event::Transcript {
                    item_id: "item".into(),
                    text: "first".into(),
                    is_final: true,
                },
                65536,
            )
            .unwrap();
        assert!(history
            .observe(
                &Event::Transcript {
                    item_id: "item".into(),
                    text: "changed".into(),
                    is_final: true
                },
                65536
            )
            .is_err());
        history.accepted.push(message(ChatRole::System, "fixed"));
        assert!(history
            .snapshot(
                &snapshot(vec![message(ChatRole::System, "changed")]),
                None,
                true,
                65536
            )
            .is_err());
    }
    #[test]
    fn configured_history_and_transcript_limits_are_not_ignored() {
        let mut history = History {
            max_messages: 1,
            max_transcripts: 1,
            ..History::default()
        };
        assert!(history
            .snapshot(
                &snapshot(vec![
                    message(ChatRole::User, "a"),
                    message(ChatRole::User, "b")
                ]),
                Some((0, 2)),
                false,
                65536
            )
            .is_err());
        history
            .observe(
                &Event::Transcript {
                    item_id: "one".into(),
                    text: "one".into(),
                    is_final: true,
                },
                65536,
            )
            .unwrap();
        assert!(history
            .observe(
                &Event::Transcript {
                    item_id: "two".into(),
                    text: "two".into(),
                    is_final: true
                },
                65536
            )
            .is_err());
    }
    #[test]
    fn empty_and_failed_responses_do_not_invent_lifecycle_frames() {
        for status in [
            ResponseStatus::Completed,
            ResponseStatus::Failed,
            ResponseStatus::Cancelled,
        ] {
            let mut mapper = Mapper::default();
            let frames = mapper
                .frames(
                    &event(Event::ResponseDone {
                        response_id: "empty".into(),
                        status,
                        usage: None,
                    }),
                    true,
                    true,
                )
                .unwrap();
            assert!(frames.is_empty());
        }
    }
    #[test]
    fn function_only_response_balances_one_llm_lifecycle_without_speech() {
        let mut mapper = Mapper::default();
        let mut frames = vec![];
        for id in ["first", "second"] {
            frames.extend(
                mapper
                    .frames(
                        &event(Event::ToolCall {
                            response_id: "tools".into(),
                            call: FunctionCall {
                                function_name: "lookup".into(),
                                tool_call_id: id.into(),
                                arguments: json!({}),
                            },
                        }),
                        true,
                        true,
                    )
                    .unwrap(),
            );
        }
        frames.extend(
            mapper
                .frames(
                    &event(Event::ResponseDone {
                        response_id: "tools".into(),
                        status: ResponseStatus::Completed,
                        usage: None,
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        assert!(matches!(frames[0].0.data, FrameData::LlmResponseStart));
        assert!(matches!(
            frames[1].0.data,
            FrameData::FunctionCallsStarted(_)
        ));
        assert!(matches!(
            frames[2].0.data,
            FrameData::FunctionCallsStarted(_)
        ));
        assert!(matches!(frames[3].0.data, FrameData::LlmResponseEnd));
        assert_eq!(frames.len(), 4);
        assert!(mapper.active.is_none());
        assert!(mapper.speaking.is_none());
    }
    #[test]
    fn function_first_then_assistant_item_shares_one_response_lifecycle() {
        let mut mapper = Mapper::default();
        let mut frames = mapper
            .frames(
                &event(Event::ToolCall {
                    response_id: "mixed".into(),
                    call: FunctionCall {
                        function_name: "lookup".into(),
                        tool_call_id: "call".into(),
                        arguments: json!({}),
                    },
                }),
                true,
                true,
            )
            .unwrap();
        assert!(mapper
            .frames(
                &event(Event::ResponseStarted {
                    response_id: "other".into(),
                }),
                true,
                true,
            )
            .is_err());
        frames.extend(
            mapper
                .frames(
                    &event(Event::ResponseStarted {
                        response_id: "mixed".into(),
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        frames.extend(
            mapper
                .frames(
                    &event(Event::Text {
                        response_id: "mixed".into(),
                        item_id: "message".into(),
                        delta: "answer".into(),
                        audio_transcript: false,
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        frames.extend(
            mapper
                .frames(
                    &event(Event::ResponseDone {
                        response_id: "mixed".into(),
                        status: ResponseStatus::Completed,
                        usage: None,
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        assert!(matches!(frames[0].0.data, FrameData::LlmResponseStart));
        assert!(matches!(
            frames[1].0.data,
            FrameData::FunctionCallsStarted(_)
        ));
        assert!(matches!(frames[2].0.data, FrameData::LlmText(_)));
        assert!(matches!(frames[3].0.data, FrameData::LlmResponseEnd));
        assert_eq!(frames.len(), 4);
        assert!(mapper.active.is_none());
        assert!(mapper.speaking.is_none());
    }
    #[test]
    fn failed_open_response_marks_real_end_without_duplicate_error() {
        let mut mapper = Mapper::default();
        mapper
            .frames(
                &event(Event::ResponseStarted {
                    response_id: "failed".into(),
                }),
                true,
                true,
            )
            .unwrap();
        let frames = mapper
            .frames(
                &event(Event::ResponseDone {
                    response_id: "failed".into(),
                    status: ResponseStatus::Failed,
                    usage: None,
                }),
                true,
                true,
            )
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert!(matches!(frames[0].0.data, FrameData::LlmResponseEnd));
        assert_eq!(frames[0].0.metadata.get("failed"), Some(&json!(true)));
        assert!(mapper.active.is_none());
    }
    #[test]
    fn multiple_audio_items_share_balanced_lifecycle_and_transcript_appends_once() {
        let mut mapper = Mapper::default();
        let mut frames = mapper
            .frames(
                &event(Event::ResponseStarted {
                    response_id: "response".into(),
                }),
                true,
                true,
            )
            .unwrap();
        for item_id in ["first", "second"] {
            frames.extend(
                mapper
                    .frames(
                        &event(Event::Audio {
                            response_id: "response".into(),
                            item_id: item_id.into(),
                            content_index: 0,
                            audio: vec![0, 1, 2, 3],
                        }),
                        true,
                        true,
                    )
                    .unwrap(),
            );
        }
        frames.extend(
            mapper
                .frames(
                    &event(Event::Text {
                        response_id: "response".into(),
                        item_id: "first".into(),
                        delta: "hello".into(),
                        audio_transcript: true,
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        frames.extend(
            mapper
                .frames(
                    &event(Event::ResponseDone {
                        response_id: "response".into(),
                        status: ResponseStatus::Completed,
                        usage: None,
                    }),
                    true,
                    true,
                )
                .unwrap(),
        );
        assert_eq!(
            frames
                .iter()
                .filter(|(frame, _)| matches!(frame.data, FrameData::LlmResponseStart))
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|(frame, _)| matches!(frame.data, FrameData::LlmResponseEnd))
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|(frame, _)| matches!(frame.data, FrameData::TtsStarted(_)))
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|(frame, _)| matches!(frame.data, FrameData::TtsStopped(_)))
                .count(),
            1
        );
        assert!(frames.iter().any(
            |(frame, _)| matches!(&frame.data,FrameData::LlmText(text) if !text.append_to_context)
        ));
        assert_eq!(frames.iter().filter(|(frame,_)|matches!(&frame.data,FrameData::TtsText(text) if text.text.append_to_context)).count(),1);
        assert!(mapper.close().is_empty());
    }
}
