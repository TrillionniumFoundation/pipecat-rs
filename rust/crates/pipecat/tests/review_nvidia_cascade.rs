//! Actual localhost NVIDIA SSE drives native text-context and HTTP TTS branches.
use pipecat::{
    core::{
        async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
        ProcessorContext, ProcessorError, WorkerState,
    },
    pipeline::ParallelPipeline,
    services::{
        presets::{OpenAiPreset, OpenAiPresetConfig},
        ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Role,
    },
    speech::{HttpTtsBackend, HttpTtsProcessor, SpeechClient, SpeechConfig, SynthesisRequest},
    text::LlmTextProcessor,
    turns::{AssistantAggregatorConfig, LLMAssistantAggregator, SharedContext},
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};

async fn request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}

struct Capture {
    frames: Arc<Mutex<Vec<Frame>>>,
    complete: Arc<Notify>,
    context_boundary: bool,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "NvidiaReviewCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
            let boundary = if self.context_boundary {
                matches!(
                    frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            } else {
                matches!(frame.data, FrameData::LlmResponseEnd)
            };
            if boundary {
                self.complete.notify_one();
            }
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_inline_and_structured_thoughts_never_reach_real_tts_request_or_assistant_history() {
    let llm_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tts_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut llm_config = ClientConfig::openai_compatible(
        OpenAiPresetConfig::new(OpenAiPreset::Nvidia),
        "synthetic-nim-review",
    )
    .unwrap();
    llm_config.endpoint = format!(
        "http://{}/v1/chat/completions",
        llm_listener.local_addr().unwrap()
    );
    llm_config.request_timeout = Duration::from_secs(5);
    let mut speech_config = SpeechConfig::openai("synthetic-tts-review");
    speech_config.base_url = format!("http://{}/v1", tts_listener.local_addr().unwrap());
    speech_config.timeout = Duration::from_secs(5);
    let llm_peer = tokio::spawn(async move {
        let (mut socket, _) = llm_listener.accept().await.unwrap();
        request(&mut socket).await;
        let events = [
            json!({"choices":[{"delta":{"reasoning_content":"PRIVATE-STRUCTURED", "content":"<think>PRIVATE-INLINE</think>Public answer."}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ];
        let mut body: String = events
            .into_iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        body.push_str("data: [DONE]\n\n");
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    let tts_peer = tokio::spawn(async move {
        let (mut socket, _) = tts_listener.accept().await.unwrap();
        let input = request(&mut socket).await;
        assert_eq!(input["input"], "Public answer.");
        assert!(!input.to_string().contains("PRIVATE-"));
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nContent-Length: 480\r\nConnection: close\r\n\r\n").await.unwrap();
        socket.write_all(&[0u8; 480]).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), tts_listener.accept())
                .await
                .is_err(),
            "thought generated another synthesis request"
        );
    });
    let context = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig::default(),
    ));
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(llm_config).unwrap(),
        CompletionRequest::new(
            "review",
            Context::new(vec![Message::text(Role::User, "question")]),
        ),
    ));
    let tts = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::OpenAi {
                client: SpeechClient::new(speech_config).unwrap(),
                request: SynthesisRequest::new("overwritten"),
            },
            4,
        )
        .unwrap(),
    );
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = 24000;
    let frames = Arc::new(Mutex::new(Vec::new()));
    let complete = Arc::new(Notify::new());
    let context_complete = Arc::new(Notify::new());
    let capture = Arc::new(Capture {
        frames: frames.clone(),
        complete: complete.clone(),
        context_boundary: false,
    });
    let context_capture = Arc::new(Capture {
        frames: frames.clone(),
        complete: context_complete.clone(),
        context_boundary: true,
    });
    // Text history records generated output; neither branch claims heard playback.
    let branches: Vec<Vec<Arc<dyn FrameProcessor>>> = vec![
        vec![assistant, context_capture],
        vec![Arc::new(LlmTextProcessor::default()), tts, capture],
    ];
    let mut worker = Pipeline::new(vec![
        llm,
        Arc::new(ParallelPipeline::new(branches).unwrap()),
    ])
    .start(config)
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(7), async {
        tokio::join!(complete.notified(), context_complete.notified());
        worker.end().await.unwrap();
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
        llm_peer.await.unwrap();
        tts_peer.await.unwrap();
    })
    .await
    .expect("cascade completion or owned teardown exceeded deadline");
    let mut thoughts = String::new();
    let mut audio = 0usize;
    let mut starts = 0usize;
    let mut ends = 0usize;
    for frame in frames.lock().unwrap().iter() {
        match &frame.data {
            FrameData::LlmThoughtStart => starts += 1,
            FrameData::LlmThoughtEnd => ends += 1,
            FrameData::LlmThoughtText(text) => {
                assert_eq!(text.skip_tts, Some(true));
                assert!(!text.append_to_context);
                thoughts.push_str(&text.text);
            }
            FrameData::TtsAudio(pcm) => audio += pcm.audio.len(),
            FrameData::LlmResponseEnd => {
                assert_eq!(frame.metadata.get("failed"), Some(&json!(false)))
            }
            FrameData::Error(error) => panic!("cascade failed: {}", error.error),
            _ => {}
        }
    }
    assert_eq!((starts, ends), (2, 2));
    assert_eq!(thoughts, "PRIVATE-STRUCTUREDPRIVATE-INLINE");
    assert_eq!(audio, 480);
    let history = context.snapshot().unwrap();
    assert_eq!(history.messages.len(), 1);
    assert_eq!(
        history.messages[0],
        Message::text(Role::Assistant, "Public answer.")
    );
    assert!(!serde_json::to_string(&history)
        .unwrap()
        .contains("PRIVATE-"));
}
