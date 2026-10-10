use crate::transport::BrowserSender;
use pipecat_audio::SincResampler;
use pipecat_core::*;
use pipecat_rtvi::{Observation, RtviObserver};
use serde_json::json;
use std::sync::{Arc, Mutex};

fn error(e: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(e.to_string())
}
/// All input paths, including RTVI raw-audio, pass through the same format fence.
/// A capture stream may not change format in flight. Interruption preserves its filter state.
pub struct NormalizeAudio {
    rate: u32,
    state: Mutex<Option<SincResampler>>,
}
impl NormalizeAudio {
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            state: Mutex::new(None),
        }
    }
    async fn flush(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let bytes = self
            .state
            .lock()
            .map_err(|_| error("audio state poisoned"))?
            .take()
            .map(|mut resampler| resampler.flush().map_err(error))
            .transpose()?
            .unwrap_or_default();
        if !bytes.is_empty() {
            ctx.push(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(bytes, self.rate, 1).map_err(error)?,
                )),
                Direction::Downstream,
            )
            .await?;
        }
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for NormalizeAudio {
    fn name(&self) -> &str {
        "BrowserAudioNormalizer"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if frame.data.is_cancel() {
                self.state
                    .lock()
                    .map_err(|_| error("audio state poisoned"))?
                    .take();
            } else if frame.is_terminal() {
                self.flush(ctx).await?;
            }
            if let FrameData::InputAudio(audio) = &mut frame.data {
                if !(8000..=96000).contains(&audio.sample_rate)
                    || audio.num_channels != 1
                    || audio.audio.is_empty()
                    || audio.audio.len() > 64 * 1024
                    || !audio.audio.len().is_multiple_of(2)
                {
                    return Err(error(
                        "browser audio must be bounded mono PCM16 at 8–96 kHz",
                    ));
                }
                let bytes = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| error("audio state poisoned"))?;
                    if state.is_none() {
                        *state = Some(
                            SincResampler::new(audio.sample_rate, self.rate, 1).map_err(error)?,
                        );
                    }
                    let resampler = state.as_mut().unwrap();
                    if resampler.input_format().sample_rate() != audio.sample_rate {
                        return Err(error("capture rate changed during session"));
                    }
                    resampler.process(&audio.audio).map_err(error)?
                };
                if bytes.is_empty() {
                    return Ok(());
                }
                *audio = AudioRawFrame::new(bytes, self.rate, 1).map_err(error)?;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.flush(ctx).await
    }
}

/// Shared admission budget for text that can grow conversation state. Counting
/// repeated observations conservatively avoids an unbounded identity cache.
#[derive(Default)]
pub struct RetentionBudget {
    bytes: std::sync::atomic::AtomicUsize,
}
impl RetentionBudget {
    const LIMIT: usize = 4 * 1024 * 1024;
    fn admit(&self, data: &FrameData) -> Result<(), ProcessorError> {
        if !matches!(
            data,
            FrameData::InputText(_)
                | FrameData::Text(_)
                | FrameData::LlmText(_)
                | FrameData::Transcription(_)
                | FrameData::TranscriptionDetailed(_)
                | FrameData::InterimTranscription(_)
                | FrameData::InterimTranscriptionDetailed(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdate(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::UserFileDetailed(_)
                | FrameData::UserImage(_)
                | FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
        ) {
            return Ok(());
        }
        struct Count(usize);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                if self.0 > RetentionBudget::LIMIT {
                    return Err(std::io::Error::other("session text budget exceeded"));
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(0);
        serde_json::to_writer(&mut count, data).map_err(error)?;
        self.bytes
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |old| {
                    old.checked_add(count.0)
                        .filter(|total| *total <= Self::LIMIT)
                },
            )
            .map_err(|_| error("session cumulative text budget exceeded"))?;
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for RetentionBudget {
    fn name(&self) -> &str {
        "BrowserContextBudget"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.admit(&frame.data)?;
        ctx.push(frame, direction).await
    }
}

/// Observations are generation/synthesis events. No tap supplies playback timing.
pub struct EventTap {
    pub observer: Arc<RtviObserver>,
    pub sender: BrowserSender,
    pub name: &'static str,
}
#[async_trait]
impl FrameProcessor for EventTap {
    fn name(&self) -> &str {
        self.name
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Native flush barriers carry local completion state and have no RTVI
        // projection. Forward them intact; never serialize their event handle.
        if matches!(
            frame.data,
            FrameData::Flush(_) | FrameData::FlushDetailed(_)
        ) {
            return ctx.push(frame, direction).await;
        }
        for event in self
            .observer
            .observe(&frame, Observation::new(self.name, direction))?
        {
            self.sender
                .message(serde_json::to_value(event).map_err(error)?)
                .await?;
        }
        ctx.push(frame, direction).await
    }
}
#[derive(Default)]
struct GeneratedState {
    text: String,
    segment: Option<u64>,
}
/// A stable segment is updated from generated LLM text. Its `spoken:false` is
/// intentional: transport queueing and TTS timestamps are not playback evidence.
pub struct GeneratedText {
    state: Mutex<GeneratedState>,
    sender: BrowserSender,
}
impl GeneratedText {
    pub fn new(sender: BrowserSender) -> Self {
        Self {
            state: Mutex::new(GeneratedState::default()),
            sender,
        }
    }
}
#[async_trait]
impl FrameProcessor for GeneratedText {
    fn name(&self) -> &str {
        "BrowserGeneratedText"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let event = if direction == Direction::Downstream {
            let mut state = self
                .state
                .lock()
                .map_err(|_| error("text state poisoned"))?;
            match &frame.data {
                FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_) => {
                    state.text.clear();
                    state.segment = Some(frame.id);
                    None
                }
                FrameData::LlmText(text) => {
                    if state.text.len().saturating_add(text.text.len()) > 64 * 1024 {
                        return Err(error("generated response exceeds text limit"));
                    }
                    state.text.push_str(&text.text);
                    let segment = *state.segment.get_or_insert(frame.id);
                    Some(
                        json!({"label":"rtvi-ai","type":"bot-output","data":{"text":state.text,"aggregated_by":"generated","segment_id":segment,"spoken":false,"will_be_spoken":text.skip_tts!=Some(true)}}),
                    )
                }
                FrameData::Interruption | FrameData::Cancel(_) | FrameData::End(_) => {
                    state.text.clear();
                    state.segment = None;
                    None
                }
                _ => None,
            }
        } else {
            None
        };
        if let Some(value) = event {
            self.sender.message(value).await?;
        }
        ctx.push(frame, direction).await
    }
}
pub struct BrowserOutput {
    pub sender: BrowserSender,
}
#[async_trait]
impl FrameProcessor for BrowserOutput {
    fn name(&self) -> &str {
        "BrowserOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.sender.frame(&frame).await?;
        }
        ctx.push(frame, direction).await
    }
}

/// The cascade's assistant aggregator consumes LLM text. This generated-context
/// boundary preserves those same frames for the downstream synthesizer.
pub struct GeneratedContext {
    pub assistant: Arc<pipecat_turns::LLMAssistantAggregator>,
}
#[async_trait]
impl FrameProcessor for GeneratedContext {
    fn name(&self) -> &str {
        "BrowserGeneratedContext"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let preserve = matches!(
            frame.data,
            FrameData::LlmResponseStart
                | FrameData::LlmResponseEnd
                | FrameData::LlmFullResponseStart(_)
                | FrameData::LlmFullResponseEnd(_)
                | FrameData::LlmText(_)
        );
        self.assistant
            .process(frame.clone(), direction, ctx)
            .await?;
        if preserve {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.assistant.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.assistant.cleanup().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn native_flush_event_crosses_event_tap_without_becoming_client_data() {
        use pipecat_transports::{SessionConfig, WebSocketSession};
        use std::time::Duration;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(
            WebSocketSession::connect(&endpoint, SessionConfig::default()),
            async {
                WebSocketSession::accept(
                    listener.accept().await.unwrap().0,
                    SessionConfig::default(),
                )
                .await
            }
        );
        let mut client = client.unwrap();
        let server = accepted.unwrap();
        let observer = Arc::new(RtviObserver::new(Default::default(), None).unwrap());
        let worker = Pipeline::new(vec![Arc::new(EventTap {
            observer,
            sender: BrowserSender::WebSocket(server.sender()),
            name: "NativeBarrierTap",
        })])
        .start(Default::default())
        .await
        .unwrap();
        let event = LocalFlushEvent::default();
        let marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(event.clone()),
            origin: Some("pipecat.soniox.turn-start".into()),
            returning: false,
        }));
        assert!(
            serde_json::to_value(&marker.data).is_err(),
            "local authority must remain nonserializable"
        );
        let id = marker.id;
        worker
            .sender()
            .send(marker, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), event.wait())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), client.recv())
                .await
                .is_err(),
            "native marker became a browser message"
        );
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(frames.iter().any(|frame| frame.frame.id == id && matches!(&frame.frame.data, FrameData::FlushDetailed(flush) if flush.returning && flush.event.as_ref().is_some_and(LocalFlushEvent::is_set))));
        let _ = client.finish().await;
        let _ = server.finish().await;
    }

    #[tokio::test]
    async fn event_tap_still_projects_mapped_frames_and_rejects_oversized_payloads() {
        use pipecat_protocol::protobuf::{self as pb, frame::Kind};
        use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
        use std::time::Duration;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(
            WebSocketSession::connect(&endpoint, SessionConfig::default()),
            async {
                WebSocketSession::accept(
                    listener.accept().await.unwrap().0,
                    SessionConfig::default(),
                )
                .await
            }
        );
        let mut client = client.unwrap();
        let server = accepted.unwrap();
        let observer = Arc::new(
            RtviObserver::new(
                pipecat_rtvi::RtviObserverParams {
                    max_message_bytes: 1024,
                    ..Default::default()
                },
                None,
            )
            .unwrap(),
        );
        let mut worker = Pipeline::new(vec![Arc::new(EventTap {
            observer,
            sender: BrowserSender::WebSocket(server.sender()),
            name: "BoundedEventTap",
        })])
        .start(Default::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::UserStartedSpeaking),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let Packet::Binary(bytes) = tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected protobuf message")
        };
        let Some(Kind::Message(message)) = pb::Frame::parse(&bytes).unwrap().frame else {
            panic!("expected RTVI message")
        };
        let value: serde_json::Value = serde_json::from_str(&message.data).unwrap();
        assert_eq!(value["type"], "user-started-speaking");
        worker
            .sender()
            .send(
                Frame::new(FrameData::Transcription(TranscriptionFrame {
                    text: "x".repeat(2048),
                    user_id: "fixture".into(),
                    timestamp: String::new(),
                    language: None,
                    result: None,
                    finalized: true,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(error) = worker.recv().await.unwrap().frame.data.as_error_frame() {
                    assert!(
                        error.error.contains("RTVI payload too large"),
                        "{}",
                        error.error
                    );
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), client.recv())
                .await
                .is_err(),
            "oversized mapped frame reached the client"
        );
        worker.cancel("boundary verified").await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert_eq!(report.total_errors, 1);
        let _ = client.finish().await;
        let _ = server.finish().await;
    }

    #[test]
    fn repeated_non_running_text_cannot_accumulate_unbounded_context() {
        let budget = RetentionBudget::default();
        let input = FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "x".repeat(64 * 1024))],
            run_llm: false,
        });
        let mut admitted = 0;
        while budget.admit(&input).is_ok() {
            admitted += 1;
        }
        assert!(admitted > 0 && admitted < 64);
        assert!(budget.admit(&input).is_err());
        // Continuous PCM does not consume retained-text capacity.
        assert!(budget
            .admit(&FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 1024], 16000, 1).unwrap()
            ))
            .is_ok());
    }

    #[tokio::test]
    async fn capture_rate_is_stateful_and_normalized_without_relabeling() {
        let mut worker = Pipeline::new(vec![Arc::new(NormalizeAudio::new(16000))])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 3072], 48000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            let frame = worker.recv().await.unwrap();
            if let FrameData::InputAudio(audio) = frame.frame.data {
                assert_eq!(audio.sample_rate, 16000);
                assert_eq!(audio.audio, vec![0; 956]);
                break;
            }
        }
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 1024], 16000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            let frame = worker.recv().await.unwrap();
            if let Some(error) = frame.frame.data.as_error_frame() {
                assert!(error.error.contains("capture rate changed"));
                break;
            }
        }
        worker.cancel("test complete").await.unwrap();
        let _ = worker.collect().await.unwrap();
    }
    #[tokio::test]
    async fn stereo_capture_is_rejected_instead_of_reinterpreted() {
        let mut worker = Pipeline::new(vec![Arc::new(NormalizeAudio::new(16000))])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 1024], 16000, 2).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            let frame = worker.recv().await.unwrap();
            if let Some(error) = frame.frame.data.as_error_frame() {
                assert!(error.error.contains("mono PCM16"));
                break;
            }
        }
        worker.cancel("test complete").await.unwrap();
        let _ = worker.collect().await.unwrap();
    }

    #[tokio::test]
    async fn graceful_capture_end_and_stop_preserve_the_resampler_tail() {
        let pcm: Vec<u8> = (0..3072)
            .flat_map(|i| ((i % 100) as i16 * 100).to_le_bytes())
            .collect();
        let mut reference = SincResampler::new(48000, 16000, 1).unwrap();
        let mut expected = reference.process(&pcm).unwrap();
        let tail = reference.flush().unwrap();
        assert_eq!(tail.len(), 68);
        expected.extend(tail);
        for terminal in [Frame::end(), Frame::new(FrameData::Stop)] {
            let worker = Pipeline::new(vec![Arc::new(NormalizeAudio::new(16000))])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            for bytes in pcm.chunks(222) {
                worker
                    .sender()
                    .send(
                        Frame::new(FrameData::InputAudio(
                            AudioRawFrame::new(bytes.to_vec(), 48000, 1).unwrap(),
                        )),
                        Direction::Downstream,
                    )
                    .await
                    .unwrap();
            }
            worker
                .sender()
                .send(terminal, Direction::Downstream)
                .await
                .unwrap();
            let (frames, report) = worker.collect().await.unwrap();
            assert_eq!(report.total_errors, 0);
            let terminal = frames
                .iter()
                .position(|frame| frame.frame.is_terminal())
                .expect("terminal output");
            assert!(!frames[terminal..]
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::InputAudio(_))));
            let actual: Vec<_> = frames
                .into_iter()
                .filter_map(|frame| match frame.frame.data {
                    FrameData::InputAudio(audio) => Some(audio.audio),
                    _ => None,
                })
                .flatten()
                .collect();
            assert_eq!(
                actual.len(),
                2048,
                "graceful terminal discarded or duplicated normalized capture samples"
            );
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn cancelled_capture_discards_the_resampler_tail() {
        let mut worker = Pipeline::new(vec![Arc::new(NormalizeAudio::new(16000))])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 3072], 48000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            if let FrameData::InputAudio(audio) = worker.recv().await.unwrap().frame.data {
                assert_eq!(audio.audio.len(), 956);
                break;
            }
        }
        worker.cancel("discard unsubmitted capture").await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert_eq!(report.total_errors, 0);
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::InputAudio(_))));
    }
}
