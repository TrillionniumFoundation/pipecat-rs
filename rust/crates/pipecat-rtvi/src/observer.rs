use crate::{bounded_size, error, transport_frame, wire::*, RtviHandle};
use async_trait::async_trait;
use pipecat_core::*;
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone)]
pub struct RtviObserverParams {
    pub bot_output_enabled: bool,
    pub bot_llm_enabled: bool,
    pub bot_llm_marker_enabled: bool,
    pub bot_tts_enabled: bool,
    pub bot_speaking_enabled: bool,
    pub user_llm_enabled: bool,
    pub user_speaking_enabled: bool,
    pub vad_user_speaking_enabled: bool,
    pub user_mute_enabled: bool,
    pub user_transcription_enabled: bool,
    pub metrics_enabled: bool,
    pub ignored_sources: BTreeSet<String>,
    pub skip_aggregator_types: BTreeSet<String>,
    pub function_call_report_level: BTreeMap<String, RtviFunctionCallReportLevel>,
    pub max_message_bytes: usize,
    pub max_pending_segments: usize,
    pub max_pending_text_bytes: usize,
    pub dedup_capacity: usize,
}
impl Default for RtviObserverParams {
    fn default() -> Self {
        Self {
            bot_output_enabled: true,
            bot_llm_enabled: true,
            bot_llm_marker_enabled: false,
            bot_tts_enabled: true,
            bot_speaking_enabled: true,
            user_llm_enabled: true,
            user_speaking_enabled: true,
            vad_user_speaking_enabled: false,
            user_mute_enabled: true,
            user_transcription_enabled: true,
            metrics_enabled: true,
            ignored_sources: BTreeSet::new(),
            skip_aggregator_types: BTreeSet::new(),
            function_call_report_level: BTreeMap::new(),
            max_message_bytes: 1024 * 1024,
            max_pending_segments: 128,
            max_pending_text_bytes: 128 * 1024,
            dedup_capacity: 4096,
        }
    }
}
/// The caller must only set `after_output_transport` when the frame was pushed
/// by an output transport after it applied playback timing. Synthesis alone is
/// not evidence that audio was played.
#[derive(Debug, Clone, Copy)]
pub struct Observation<'a> {
    pub source: &'a str,
    pub direction: Direction,
    pub first_push: bool,
    pub after_output_transport: bool,
}
impl<'a> Observation<'a> {
    pub fn new(source: &'a str, direction: Direction) -> Self {
        Self {
            source,
            direction,
            first_push: true,
            after_output_transport: false,
        }
    }
}
struct PendingSegment {
    id: u64,
    text: AggregatedTextFrame,
    tts: bool,
}
struct State {
    params: RtviObserverParams,
    speaking: bool,
    transcription: String,
    pending: VecDeque<PendingSegment>,
    pending_bytes: usize,
    seen: HashSet<u64>,
    order: VecDeque<u64>,
}
/// An explicit observer: hosts can invoke `observe` from their push hook, or
/// insert shared taps at pipeline boundaries. No global hook is installed.
pub struct RtviObserver {
    state: Mutex<State>,
    session: Option<RtviHandle>,
}
impl RtviObserver {
    pub fn new(
        params: RtviObserverParams,
        session: Option<RtviHandle>,
    ) -> Result<Self, ProcessorError> {
        if params.max_message_bytes < 1024
            || params.max_pending_segments == 0
            || params.max_pending_text_bytes == 0
            || params.dedup_capacity == 0
        {
            return Err(error(
                "observer capacities must be positive; message limit must be >=1024",
            ));
        }
        bounded_size(&params.function_call_report_level, params.max_message_bytes)?;
        Ok(Self {
            state: Mutex::new(State {
                params,
                speaking: false,
                transcription: String::new(),
                pending: VecDeque::new(),
                pending_bytes: 0,
                seen: HashSet::new(),
                order: VecDeque::new(),
            }),
            session,
        })
    }
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        state.speaking = false;
        state.transcription.clear();
        state.pending.clear();
        state.pending_bytes = 0;
        state.seen.clear();
        state.order.clear();
    }
    pub fn observe(
        &self,
        frame: &Frame,
        observation: Observation<'_>,
    ) -> Result<Vec<Envelope>, ProcessorError> {
        // Provisional prompts and unconsumed local tool capabilities are not
        // client-facing transcript or function-call observations.
        if matches!(&frame.data, FrameData::LlmContextDetailed(context) if context.speculation)
            || (frame.native_speculation().is_some()
                && matches!(
                    frame.data,
                    FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallsStartedDetailed(_)
                ))
        {
            return Ok(vec![]);
        }
        let legacy = self.session.as_ref().is_some_and(|s| s.state().legacy);
        let mut state = self.state.lock().unwrap();
        if state.params.ignored_sources.contains(observation.source)
            || (frame.broadcast_sibling_id.is_some()
                && observation.direction != Direction::Downstream)
        {
            return Ok(vec![]);
        }
        let aggregated = matches!(
            frame.data,
            FrameData::AggregatedText(_)
                | FrameData::TtsText(_)
                | FrameData::AggregatedTextProgress(_)
        );
        if aggregated && !observation.after_output_transport {
            return Ok(vec![]);
        }
        if !aggregated && !observation.first_push {
            return Ok(vec![]);
        }
        if matches!(
            frame.data,
            FrameData::InputAudio(_) | FrameData::TtsAudio(_) | FrameData::OutputAudio(_)
        ) {
            return Ok(vec![]);
        }
        if state.seen.contains(&frame.id) {
            return Ok(vec![]);
        }
        // Validate before retaining either the payload or its identity.
        bounded_size(&frame.data, state.params.max_message_bytes)?;
        state.seen.insert(frame.id);
        state.order.push_back(frame.id);
        if state.order.len() > state.params.dedup_capacity {
            if let Some(id) = state.order.pop_front() {
                state.seen.remove(&id);
            }
        }
        let mut events = vec![];
        match &frame.data {
            FrameData::UserStartedSpeaking if state.params.user_speaking_enabled => {
                events.push(ServerEvent::UserStartedSpeaking)
            }
            FrameData::UserStoppedSpeaking if state.params.user_speaking_enabled => {
                events.push(ServerEvent::UserStoppedSpeaking)
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_)
                if state.params.vad_user_speaking_enabled =>
            {
                events.push(ServerEvent::VadUserStartedSpeaking)
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_)
                if state.params.vad_user_speaking_enabled =>
            {
                events.push(ServerEvent::VadUserStoppedSpeaking)
            }
            FrameData::UserMuteStarted if state.params.user_mute_enabled => {
                events.push(ServerEvent::UserMuteStarted)
            }
            FrameData::UserMuteStopped if state.params.user_mute_enabled => {
                events.push(ServerEvent::UserMuteStopped)
            }
            FrameData::BotStartedSpeaking => {
                state.speaking = true;
                if state.params.bot_speaking_enabled {
                    events.push(ServerEvent::BotStartedSpeaking);
                }
                while let Some(segment) = state.pending.pop_front() {
                    aggregate(&state.params, segment, legacy, &mut events);
                }
                state.pending_bytes = 0;
            }
            FrameData::BotStoppedSpeaking => {
                state.speaking = false;
                if state.params.bot_speaking_enabled {
                    events.push(ServerEvent::BotStoppedSpeaking);
                }
            }
            FrameData::Interruption => {
                state.pending.clear();
                state.pending_bytes = 0;
                state.transcription.clear();
                if state.params.bot_speaking_enabled {
                    events.push(ServerEvent::BotInterrupted);
                }
            }
            FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
            | FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop => {
                state.pending.clear();
                state.pending_bytes = 0;
                state.transcription.clear();
                state.speaking = false;
            }
            FrameData::Transcription(text) | FrameData::InterimTranscription(text)
                if state.params.user_transcription_enabled =>
            {
                events.push(ServerEvent::UserTranscription(TranscriptionData {
                    text: text.text.clone(),
                    user_id: text.user_id.clone(),
                    timestamp: text.timestamp.clone(),
                    is_final: matches!(frame.data, FrameData::Transcription(_)),
                }));
            }
            FrameData::TranscriptionDetailed(text)
            | FrameData::InterimTranscriptionDetailed(text)
                if state.params.user_transcription_enabled =>
            {
                events.push(ServerEvent::UserTranscription(TranscriptionData {
                    text: text.text.text.clone(),
                    user_id: text.user_id.clone(),
                    timestamp: text.timestamp.clone(),
                    is_final: matches!(frame.data, FrameData::TranscriptionDetailed(_)),
                }));
            }
            FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_)
                if state.params.bot_llm_enabled =>
            {
                events.push(ServerEvent::BotLlmStarted)
            }
            FrameData::LlmResponseEnd | FrameData::LlmFullResponseEnd(_)
                if state.params.bot_llm_enabled =>
            {
                events.push(ServerEvent::BotLlmStopped)
            }
            FrameData::LlmText(text) if state.params.bot_llm_enabled => {
                if state.transcription.len().saturating_add(text.text.len())
                    > state.params.max_pending_text_bytes
                {
                    return Err(error("bot transcription buffer limit exceeded"));
                }
                state.transcription.push_str(&text.text);
                events.push(ServerEvent::BotLlmText(TextData {
                    text: text.text.clone(),
                }));
                if pipecat_text::string::match_endofsentence(&state.transcription, None) > 0 {
                    events.push(ServerEvent::BotTranscription(TextData {
                        text: std::mem::take(&mut state.transcription),
                    }));
                }
            }
            FrameData::LlmMarkerResponse(marker) if state.params.bot_llm_marker_enabled => events
                .push(ServerEvent::BotLlmMarker(MarkerData {
                    text: marker.marker.clone().unwrap_or_default(),
                    kind: marker.kind.clone(),
                    raw: marker.raw.clone(),
                    markers: marker.markers.clone(),
                })),
            FrameData::TtsStarted(_) if state.params.bot_tts_enabled => {
                events.push(ServerEvent::BotTtsStarted)
            }
            FrameData::TtsStopped(_) if state.params.bot_tts_enabled => {
                events.push(ServerEvent::BotTtsStopped)
            }
            FrameData::AggregatedText(text) | FrameData::TtsText(text) => {
                if !state.params.bot_output_enabled && !state.params.bot_tts_enabled {
                    return Ok(vec![]);
                }
                let segment = PendingSegment {
                    id: frame.id,
                    text: text.clone(),
                    tts: matches!(frame.data, FrameData::TtsText(_)),
                };
                if state.speaking {
                    aggregate(&state.params, segment, legacy, &mut events);
                } else {
                    if state.pending.len() >= state.params.max_pending_segments
                        || state.pending_bytes.saturating_add(text.text.text.len())
                            > state.params.max_pending_text_bytes
                    {
                        return Err(error("RTVI pending segment limit exceeded"));
                    }
                    state.pending_bytes += text.text.text.len();
                    state.pending.push_back(segment);
                }
            }
            FrameData::AggregatedTextProgress(progress)
                if !legacy && state.params.bot_output_enabled =>
            {
                if !state
                    .params
                    .skip_aggregator_types
                    .contains(&progress.aggregated_by)
                {
                    events.push(ServerEvent::BotOutput(BotOutputData {
                        text: progress.text.clone(),
                        aggregated_by: progress.aggregated_by.clone(),
                        segment_id: Some(progress.segment_id),
                        spoken: None,
                        will_be_spoken: Some(true),
                        spoken_status: Some(if progress.remaining_text.is_empty() {
                            SpokenStatus::Completed
                        } else {
                            SpokenStatus::InProgress
                        }),
                        spoken_progress: Some(SpokenProgress {
                            accumulated_text: progress.accumulated_text.clone(),
                            remaining_text: progress.remaining_text.clone(),
                        }),
                    }));
                }
            }
            FrameData::LlmContext(context) if state.params.user_llm_enabled => {
                if let Some(message) = context.messages.last().filter(|m| m.role == ChatRole::User)
                {
                    events.push(ServerEvent::UserLlmText(TextData {
                        text: message.content.clone(),
                    }));
                }
            }
            FrameData::LlmContextDetailed(context) if state.params.user_llm_enabled => {
                if let Some(message) = context
                    .context
                    .messages
                    .last()
                    .filter(|m| m.role == ChatRole::User)
                {
                    let text = match &message.content {
                        MessageContent::Text(s) => s.clone(),
                        MessageContent::Parts(parts) => parts
                            .iter()
                            .filter_map(|p| {
                                if let MessagePart::Text { text } = p {
                                    Some(text.as_str())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(" "),
                    };
                    events.push(ServerEvent::UserLlmText(TextData { text }));
                }
            }
            FrameData::Metrics(metrics) if state.params.metrics_enabled => {
                events.push(ServerEvent::Metrics(metrics_data(metrics)))
            }
            FrameData::FunctionCallsStarted(calls) => {
                for call in calls {
                    report(
                        &state.params,
                        Phase::Started,
                        &call.function_name,
                        &call.tool_call_id,
                        None,
                        None,
                        &mut events,
                    );
                }
            }
            FrameData::FunctionCallsStartedDetailed(calls) => {
                for call in calls {
                    report(
                        &state.params,
                        Phase::Started,
                        &call.function_name,
                        &call.tool_call_id,
                        None,
                        None,
                        &mut events,
                    );
                }
            }
            FrameData::ExternalFunctionCallsStarted(calls) => {
                for call in calls {
                    report(
                        &state.params,
                        Phase::Started,
                        &call.function_name,
                        &call.tool_call_id,
                        None,
                        None,
                        &mut events,
                    );
                }
            }
            FrameData::FunctionCallInProgress(call) => report(
                &state.params,
                Phase::Progress,
                &call.function_name,
                &call.tool_call_id,
                Some(&call.arguments),
                None,
                &mut events,
            ),
            FrameData::ExternalFunctionCallInProgress(call) => report(
                &state.params,
                Phase::Progress,
                &call.function_name,
                &call.tool_call_id,
                Some(&call.arguments),
                None,
                &mut events,
            ),
            FrameData::FunctionCallResult(call) if call.is_final => report(
                &state.params,
                Phase::Stopped,
                &call.function_name,
                &call.tool_call_id,
                None,
                Some(&call.result),
                &mut events,
            ),
            FrameData::ExternalFunctionCallResult(call) if call.is_final => report(
                &state.params,
                Phase::Stopped,
                &call.function_name,
                &call.tool_call_id,
                None,
                Some(&call.result),
                &mut events,
            ),
            FrameData::FunctionCallResultDetailed(call)
                if call.properties.as_ref().is_none_or(|p| p.is_final) =>
            {
                report(
                    &state.params,
                    Phase::Stopped,
                    &call.function_name,
                    &call.tool_call_id,
                    None,
                    Some(&call.result),
                    &mut events,
                )
            }
            FrameData::FunctionCallCancel(call) => report(
                &state.params,
                Phase::Cancelled,
                &call.function_name,
                &call.tool_call_id,
                None,
                None,
                &mut events,
            ),
            FrameData::ExternalFunctionCallCancel(call) => report(
                &state.params,
                Phase::Cancelled,
                &call.function_name,
                &call.tool_call_id,
                None,
                None,
                &mut events,
            ),
            FrameData::RtviServerMessage(data) => {
                events.push(ServerEvent::ServerMessage(data.clone()))
            }
            FrameData::RtviUiCommand(data) => events.push(ServerEvent::UiCommand(UiCommandData {
                command: data.command.clone(),
                payload: data.payload.clone(),
            })),
            FrameData::RtviUiJobGroup(Some(data)) => events.push(ServerEvent::UiJobGroup(
                serde_json::from_value(data.clone()).map_err(error)?,
            )),
            FrameData::RtviConfigureObserver(config) => {
                if let Some(levels) = &config.function_call_report_level {
                    state.params.function_call_report_level = levels.clone();
                }
                if let Some(value) = config.vad_user_speaking_enabled {
                    state.params.vad_user_speaking_enabled = value;
                }
                if let Some(value) = config.bot_llm_marker_enabled {
                    state.params.bot_llm_marker_enabled = value;
                }
            }
            FrameData::RtviServerResponse(response) => {
                let event = match &response.error {
                    Some(error) => ServerEvent::ErrorResponse(ErrorResponseData {
                        error: error.clone(),
                    }),
                    None => ServerEvent::ServerResponse(CustomMessageData {
                        t: response.client_msg.message_type.clone(),
                        d: response.data.clone(),
                    }),
                };
                let envelope =
                    Envelope::response(&response.client_msg.msg_id, &event).map_err(error)?;
                bounded_size(&envelope, state.params.max_message_bytes)?;
                return Ok(vec![envelope]);
            }
            _ => {}
        }
        let envelopes = events
            .into_iter()
            .map(|e| Envelope::server(&e).map_err(error))
            .collect::<Result<Vec<_>, _>>()?;
        for envelope in &envelopes {
            bounded_size(envelope, state.params.max_message_bytes)?;
        }
        Ok(envelopes)
    }
    /// Create a frame-preserving pipeline tap. Normal taps never claim playback
    /// completion. Output-transport timing must be supplied with `observe`.
    pub fn tap(self: &Arc<Self>, source: impl Into<String>) -> RtviObserverTap {
        RtviObserverTap {
            observer: self.clone(),
            source: source.into(),
        }
    }
}
pub struct RtviObserverTap {
    observer: Arc<RtviObserver>,
    source: String,
}
#[async_trait]
impl FrameProcessor for RtviObserverTap {
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
            .observe(&frame, Observation::new(&self.source, direction))?;
        ctx.push(frame, direction).await?;
        let limit = self.observer.state.lock().unwrap().params.max_message_bytes;
        for envelope in events {
            ctx.push(transport_frame(envelope, limit)?, Direction::Downstream)
                .await?;
        }
        Ok(())
    }
}
fn aggregate(
    params: &RtviObserverParams,
    segment: PendingSegment,
    legacy: bool,
    events: &mut Vec<ServerEvent>,
) {
    let frame = segment.text;
    if params.skip_aggregator_types.contains(&frame.aggregated_by) {
        return;
    }
    if params.bot_output_enabled
        && (legacy || !matches!(frame.aggregated_by.as_str(), "word" | "token"))
    {
        let text = frame.text.text.clone();
        let progress = frame.will_be_spoken.then(|| SpokenProgress {
            accumulated_text: if segment.tts {
                text.clone()
            } else {
                String::new()
            },
            remaining_text: if segment.tts {
                String::new()
            } else {
                text.clone()
            },
        });
        events.push(ServerEvent::BotOutput(BotOutputData {
            text,
            aggregated_by: frame.aggregated_by,
            segment_id: Some(segment.id),
            spoken: Some(segment.tts),
            will_be_spoken: Some(frame.will_be_spoken),
            spoken_status: frame.will_be_spoken.then_some(if segment.tts {
                SpokenStatus::Completed
            } else {
                SpokenStatus::New
            }),
            spoken_progress: progress,
        }));
    }
    if segment.tts && params.bot_tts_enabled {
        events.push(ServerEvent::BotTtsText(TextData {
            text: frame.text.text,
        }));
    }
}
#[derive(Clone, Copy)]
enum Phase {
    Started,
    Progress,
    Stopped,
    Cancelled,
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}
fn report(
    params: &RtviObserverParams,
    phase: Phase,
    name: &str,
    id: &str,
    arguments: Option<&Value>,
    result: Option<&Value>,
    events: &mut Vec<ServerEvent>,
) {
    use RtviFunctionCallReportLevel as Level;
    let level = params
        .function_call_report_level
        .get(name)
        .or_else(|| params.function_call_report_level.get("*"))
        .copied()
        .unwrap_or_default();
    if level == Level::Disabled {
        return;
    }
    let function_name =
        matches!(level, Level::Name | Level::Arguments | Level::Full).then(|| name.to_owned());
    events.push(match phase {
        Phase::Started => {
            ServerEvent::LlmFunctionCallStarted(FunctionCallStartedData { function_name })
        }
        Phase::Progress => ServerEvent::LlmFunctionCallInProgress(FunctionCallInProgressData {
            function_name,
            tool_call_id: id.into(),
            arguments: if matches!(level, Level::Arguments | Level::Full) {
                arguments.and_then(Value::as_object).cloned()
            } else {
                None
            },
        }),
        Phase::Stopped | Phase::Cancelled => {
            ServerEvent::LlmFunctionCallStopped(FunctionCallStoppedData {
                function_name,
                tool_call_id: id.into(),
                cancelled: matches!(phase, Phase::Cancelled),
                result: if level == Level::Full && matches!(phase, Phase::Stopped) {
                    result.filter(|v| truthy(v)).cloned()
                } else {
                    None
                },
            })
        }
    });
}
fn metrics_data(metrics: &[Metric]) -> Map<String, Value> {
    let mut output = Map::new();
    for metric in metrics {
        let (key, data) = match metric {
            Metric::Ttfb { processor, seconds } => {
                ("ttfb", json!({"processor":processor,"value":seconds}))
            }
            Metric::Processing { processor, seconds } => {
                ("processing", json!({"processor":processor,"value":seconds}))
            }
            Metric::LlmUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..
            } => (
                "tokens",
                json!({"prompt_tokens":prompt_tokens,"completion_tokens":completion_tokens,"total_tokens":total_tokens}),
            ),
            Metric::TtsUsage {
                processor,
                characters,
            } => (
                "characters",
                json!({"processor":processor,"value":characters}),
            ),
            Metric::SttUsage {
                processor,
                audio_seconds,
            } => (
                "stt_usage",
                json!({"processor":processor,"value":{"audio_seconds":audio_seconds}}),
            ),
        };
        output
            .entry(key)
            .or_insert_with(|| Value::Array(vec![]))
            .as_array_mut()
            .expect("metric arrays")
            .push(data);
    }
    output
}
