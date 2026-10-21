//! Explicit, tool-free browser speech-to-speech composition. No cascade clients.
use crate::RunnerError;
use pipecat_realtime::{
    RealtimeClient, RealtimeConfig, RealtimeLimits, TranscriptionConfig, TurnDetection,
};

/// One native OpenAI Realtime connection per accepted browser capability.
/// Endpoints are trusted application configuration, never browser request data.
/// PCM format and server VAD are intentionally fixed by this route.
#[derive(Clone)]
pub struct OpenAiRealtimeVoiceConfig {
    api_key: String,
    pub endpoint: String,
    pub model: String,
    pub voice: String,
    pub instructions: String,
    pub limits: RealtimeLimits,
}
impl std::fmt::Debug for OpenAiRealtimeVoiceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiRealtimeVoiceConfig")
            .field("api_key", &"[REDACTED]")
            .field("endpoint", &"[configured]")
            .finish_non_exhaustive()
    }
}
impl OpenAiRealtimeVoiceConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        let native = RealtimeConfig::new("");
        Self {
            api_key: api_key.into(),
            endpoint: native.endpoint,
            model: native.model,
            voice: "alloy".into(),
            instructions: pipecat_cli::config::DEFAULT_SYSTEM.into(),
            limits: RealtimeLimits {
                queue_capacity: 16,
                max_message_bytes: 128 * 1024,
                max_audio_chunk_bytes: 64 * 1024,
                connect_timeout: std::time::Duration::from_secs(5),
                operation_timeout: std::time::Duration::from_secs(2),
                close_timeout: std::time::Duration::from_secs(1),
                ..Default::default()
            },
        }
    }
    pub fn from_env() -> Result<Self, RunnerError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }
    /// The selected route never reads separate STT/TTS credentials.
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, RunnerError> {
        for key in [
            "PIPECAT_VOICE_CASCADE",
            "PIPECAT_STT_PROVIDER",
            "PIPECAT_WHISPER_MODEL",
            "PIPECAT_WHISPER_SEGMENT_MS",
            "PIPECAT_WHISPER_THREADS",
            "PIPECAT_WHISPER_LANGUAGE",
            "PIPECAT_WHISPER_PRE_ROLL_MS",
            "PIPECAT_INPUT_SAMPLE_RATE",
            "PIPECAT_OUTPUT_SAMPLE_RATE",
            "SONIOX_STT_MODEL",
            "SONIOX_TTS_MODEL",
            "SONIOX_VOICE",
            "CARTESIA_VOICE_ID",
            "OPENAI_MODEL",
        ] {
            if get(key).is_some() {
                return Err(RunnerError::RealtimeConfiguration("cascade, local STT and custom rate options are incompatible with --realtime openai"));
            }
        }
        let mut config = Self::new(get("OPENAI_API_KEY").ok_or(
            RunnerError::RealtimeConfiguration("OPENAI_API_KEY is required"),
        )?);
        if let Some(value) = get("OPENAI_REALTIME_MODEL") {
            config.model = value;
        }
        if let Some(value) = get("OPENAI_REALTIME_VOICE") {
            config.voice = value;
        }
        if let Some(value) = get("PIPECAT_SYSTEM_PROMPT") {
            config.instructions = value;
        }
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), RunnerError> {
        if self.api_key.trim().is_empty()
            || self.api_key.len() > 16384
            || self.api_key.chars().any(char::is_control)
            || self.instructions.len() > 64 * 1024
            || self.voice.is_empty()
            || self.voice.len() > 128
            || self.limits.connect_timeout > std::time::Duration::from_secs(5)
            || self.limits.operation_timeout > std::time::Duration::from_secs(2)
            || self.limits.close_timeout > std::time::Duration::from_secs(1)
            || self.limits.max_message_bytes > 128 * 1024
            || self.limits.max_audio_chunk_bytes > 64 * 1024
            || self.limits.queue_capacity > 64
        {
            return Err(RunnerError::RealtimeConfiguration(
                "realtime voice configuration exceeds bounded session limits",
            ));
        }
        self.native()
            .validate()
            .map_err(|_| RunnerError::RealtimeConfiguration("invalid realtime voice configuration"))
    }
    fn native(&self) -> RealtimeConfig {
        let mut config = RealtimeConfig::new(self.api_key.clone());
        config.endpoint = self.endpoint.clone();
        config.model = self.model.clone();
        config.limits = self.limits.clone();
        config.session.instructions = Some(self.instructions.clone());
        let audio = config
            .session
            .audio
            .as_mut()
            .expect("native PCM configuration");
        let input = audio.input.as_mut().expect("native PCM input");
        input.turn_detection = Some(TurnDetection::ServerVad {
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 500,
            create_response: true,
            interrupt_response: true,
        });
        input.transcription = Some(TranscriptionConfig {
            model: "gpt-4o-mini-transcribe".into(),
            language: None,
            prompt: None,
        });
        audio.output.as_mut().expect("native PCM output").voice = Some(self.voice.clone());
        config
    }
    pub(crate) fn client(&self) -> Result<RealtimeClient, RunnerError> {
        self.validate()?;
        RealtimeClient::new(self.native()).map_err(|_| RunnerError::Provider)
    }
}

use crate::{
    processors::{GeneratedText, RetentionBudget},
    transport::BrowserSender,
};
use pipecat_core::*;
use pipecat_rtvi::{Observation, RtviObserver};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, SharedContext,
};
use std::sync::Arc;

pub(crate) fn compose(
    config: &OpenAiRealtimeVoiceConfig,
    processors: &mut Vec<Arc<dyn FrameProcessor>>,
    output: BrowserSender,
    observer: Arc<RtviObserver>,
) -> Result<
    (
        SharedContext,
        tokio::sync::watch::Receiver<pipecat_realtime::RealtimeConnectionState>,
    ),
    RunnerError,
> {
    // Empty shared history: instructions belong only to session.update. No seed
    // is imported twice and no implicit startup inference is requested.
    let pair = LLMContextAggregatorPair::new(
        Default::default(),
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Text,
        },
    );
    let provider = Arc::new(
        pipecat_realtime::OpenAiRealtimeProcessor::new(config.client()?)
            .map_err(|_| RunnerError::Provider)?
            .with_interruption_resilient_user_input(),
    );
    let state = provider.subscribe_connection_state();
    let budget = Arc::new(RetentionBudget::default());
    let events = |name| {
        Arc::new(Events {
            observer: observer.clone(),
            sender: output.clone(),
            name,
        }) as Arc<dyn FrameProcessor>
    };
    processors.extend([
        budget.clone() as Arc<dyn FrameProcessor>,
        pair.user(),
        // Upstream server transcripts must be bounded and observed BEFORE the
        // paired user consumes them. Do not change their direction.
        budget.clone(),
        events("RealtimeUserEvents"),
        provider,
        budget,
        Arc::new(NoTools),
        events("RealtimeOutputEvents"),
        Arc::new(GeneratedText::new_realtime(output.clone())),
        Arc::new(Output { sender: output }),
        pair.assistant(),
    ]);
    Ok((pair.context(), state))
}

/// Reject unsupported controls before generic RTVI can mutate paired history.
/// The only composition/endpoint selection is trusted constructor/CLI input.
pub(crate) fn admit_browser(frame: &Frame) -> Result<(), RunnerError> {
    if let FrameData::InputTransportMessage(value) = &frame.data {
        let envelope: pipecat_protocol::rtvi::Envelope = serde_json::from_value(value.clone())
            .map_err(|_| RunnerError::Protocol("invalid realtime browser envelope"))?;
        envelope
            .client_command()
            .map_err(|_| RunnerError::Protocol("invalid realtime browser command"))?;
        let kind = value["type"].as_str().unwrap_or_default();
        if !matches!(
            kind,
            "client-ready" | "disconnect-bot" | "send-text" | "raw-audio" | "raw-audio-batch"
        ) {
            return Err(RunnerError::Protocol(
                "unsupported realtime browser control",
            ));
        }
        if kind == "send-text" && value["data"]["options"]["audio_response"] == false {
            return Err(RunnerError::Protocol(
                "realtime voice requires audio response mode",
            ));
        }
    }
    Ok(())
}
struct NoTools;
#[async_trait]
impl FrameProcessor for NoTools {
    fn name(&self) -> &str {
        "RealtimeVoiceNoTools"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallsStartedDetailed(_)
        ) {
            return Err(ProcessorError::fatal(
                "tools are unsupported by realtime browser voice",
            ));
        }
        ctx.push(frame, direction).await
    }
}
struct Events {
    observer: Arc<RtviObserver>,
    sender: BrowserSender,
    name: &'static str,
}
#[async_trait]
impl FrameProcessor for Events {
    fn name(&self) -> &str {
        self.name
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !matches!(
            frame.data,
            FrameData::Flush(_) | FrameData::FlushDetailed(_)
        ) {
            for event in self
                .observer
                .observe(&frame, Observation::new(self.name, direction))?
            {
                let value = serde_json::to_value(event)
                    .map_err(|_| ProcessorError::new("realtime event serialization"))?;
                if direction == Direction::Downstream {
                    self.sender
                        .message_fenced(value, ctx.invocation_fence()?)
                        .await?;
                } else {
                    self.sender.message(value).await?;
                }
            }
        }
        ctx.push(frame, direction).await
    }
}
struct Output {
    sender: BrowserSender,
}
#[async_trait]
impl FrameProcessor for Output {
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        match &self.sender {
            BrowserSender::WebSocket(sender) => {
                tokio::time::timeout(std::time::Duration::from_secs(5), sender.flush())
                    .await
                    .map_err(|_| ProcessorError::new("realtime browser drain deadline"))?
                    .map_err(|_| ProcessorError::new("realtime browser drain failed"))
            }
            #[cfg(feature = "webrtc")]
            BrowserSender::WebRtc(_) => Err(ProcessorError::new(
                "realtime browser voice supports WebSocket only",
            )),
        }
    }
    fn name(&self) -> &str {
        "RealtimeBrowserOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) = &frame.data {
                if audio.sample_rate != 24000 || audio.num_channels != 1 {
                    return Err(ProcessorError::fatal(
                        "realtime output must be 24000 Hz mono PCM",
                    ));
                }
                let packet = crate::wire::audio(&frame, audio)
                    .map_err(|_| ProcessorError::new("realtime audio framing"))?;
                self.sender
                    .packet_fenced(packet, ctx.invocation_fence()?)
                    .await?;
            } else {
                self.sender.frame(&frame).await?;
            }
        }
        ctx.push(frame, direction).await
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;
    use futures_util::StreamExt;
    use pipecat_protocol::protobuf::{self as pb, frame::Kind};
    use pipecat_transports::{SessionConfig, WebSocketSession};
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_tungstenite::{
        tungstenite::{protocol::Role, Message},
        WebSocketStream,
    };

    // Keep the raw peer alive until the owner flushes its Close reply and joins.
    // Two locally finishing session owners may drop a duplex endpoint before
    // the other can write its reply; that is not the output contract under test.
    async fn close_peer_and_join(
        mut session: WebSocketSession,
        mut peer: WebSocketStream<tokio::io::DuplexStream>,
    ) {
        timeout(Duration::from_secs(2), peer.close(None))
            .await
            .unwrap()
            .unwrap();
        assert!(timeout(Duration::from_secs(2), session.recv())
            .await
            .unwrap()
            .is_none());
        timeout(Duration::from_secs(2), session.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(2), peer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Close(_)
        ));
    }

    #[tokio::test]
    async fn actual_realtime_output_drains_graceful_tail_before_pipeline_retirement() {
        for stop in [false, true] {
            let (write, read) = tokio::io::duplex(64);
            let config = SessionConfig::default();
            let socket = WebSocketStream::from_raw_socket(
                write,
                Role::Server,
                Some(config.websocket_config().unwrap()),
            )
            .await;
            let session = WebSocketSession::from_websocket(socket, config.clone()).unwrap();
            let mut peer = WebSocketStream::from_raw_socket(
                read,
                Role::Client,
                Some(config.websocket_config().unwrap()),
            )
            .await;
            let sender = BrowserSender::WebSocket(session.sender());
            let mut worker = Pipeline::new(vec![
                Arc::new(GeneratedText::new_realtime(sender.clone())),
                Arc::new(Output { sender }),
            ])
            .start(Default::default())
            .await
            .unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::LlmResponseStart),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::LlmText(TextFrame::llm("tail text"))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::TtsAudio(
                        AudioRawFrame::new(vec![4; 65536], 24000, 1).unwrap(),
                    )),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            loop {
                if matches!(
                    worker.recv().await.unwrap().frame.data,
                    FrameData::TtsAudio(_)
                ) {
                    break;
                }
            }
            if stop {
                worker
                    .sender()
                    .send(Frame::new(FrameData::Stop), Direction::Downstream)
                    .await
                    .unwrap();
            } else {
                worker.end().await.unwrap();
            }
            let mut collect = tokio::spawn(async move { worker.collect().await.unwrap() });
            assert!(timeout(Duration::from_millis(50), &mut collect)
                .await
                .is_err());
            let mut pcm = 0;
            let mut text = false;
            while pcm == 0 || !text {
                let Message::Binary(bytes) = timeout(Duration::from_secs(2), peer.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                else {
                    panic!("protobuf required")
                };
                match pb::Frame::parse(&bytes).unwrap().frame.unwrap() {
                    Kind::Audio(audio) => {
                        assert_eq!(audio.audio, vec![4; 65536]);
                        pcm += audio.audio.len();
                    }
                    Kind::Message(message) => {
                        let value: serde_json::Value = serde_json::from_str(&message.data).unwrap();
                        assert_eq!(value["data"]["text"], "tail text");
                        assert_eq!(value["data"]["spoken"], false);
                        text = true;
                    }
                    _ => panic!("unexpected output"),
                }
            }
            let (_, report) = timeout(Duration::from_secs(2), collect)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(report.total_errors, 0);
            close_peer_and_join(session, peer).await;
        }
    }

    #[tokio::test]
    async fn actual_realtime_output_discards_queued_old_text_after_interruption() {
        let (write, read) = tokio::io::duplex(64);
        let config = SessionConfig::default();
        let socket = WebSocketStream::from_raw_socket(
            write,
            Role::Server,
            Some(config.websocket_config().unwrap()),
        )
        .await;
        let session = WebSocketSession::from_websocket(socket, config.clone()).unwrap();
        let mut peer = WebSocketStream::from_raw_socket(
            read,
            Role::Client,
            Some(config.websocket_config().unwrap()),
        )
        .await;
        let sender = BrowserSender::WebSocket(session.sender());
        let mut worker = Pipeline::new(vec![
            Arc::new(GeneratedText::new_realtime(sender.clone())),
            Arc::new(Output { sender }),
        ])
        .start(Default::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::TtsAudio(
                    AudioRawFrame::new(vec![5; 65536], 24000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            if matches!(
                worker.recv().await.unwrap().frame.data,
                FrameData::TtsAudio(_)
            ) {
                break;
            }
        }
        // The first already-admitted audio is irrevocable. The following text
        // is deliberately queued behind its blocked flush and must be retired.
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmText(TextFrame::llm("stale queued text"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            if matches!(
                worker.recv().await.unwrap().frame.data,
                FrameData::LlmText(_)
            ) {
                break;
            }
        }
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        loop {
            if matches!(
                worker.recv().await.unwrap().frame.data,
                FrameData::Interruption
            ) {
                break;
            }
        }
        loop {
            let Message::Binary(bytes) = timeout(Duration::from_secs(2), peer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
            else {
                panic!("protobuf required")
            };
            match pb::Frame::parse(&bytes).unwrap().frame.unwrap() {
                Kind::Audio(audio) => assert_eq!(audio.audio, vec![5; 65536]),
                Kind::Interruption(_) => break,
                Kind::Message(_) => panic!("stale generated text escaped queue fence"),
                _ => panic!("unexpected output"),
            }
        }
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmText(TextFrame::llm("fresh"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let Message::Binary(bytes) = timeout(Duration::from_secs(2), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        else {
            panic!("protobuf required")
        };
        let Kind::Message(message) = pb::Frame::parse(&bytes).unwrap().frame.unwrap() else {
            panic!("fresh text absent")
        };
        let value: serde_json::Value = serde_json::from_str(&message.data).unwrap();
        assert_eq!(value["data"]["text"], "fresh");
        worker.cancel("verified").await.unwrap();
        worker.collect().await.unwrap();
        close_peer_and_join(session, peer).await;
    }
}
