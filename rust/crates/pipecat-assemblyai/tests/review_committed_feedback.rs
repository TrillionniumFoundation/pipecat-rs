//! Credential-free independent tests of committed assistant feedback consumers.
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use pipecat_assemblyai::sync::{
    AssemblyAiSyncConfig, AssemblyAiSyncProcessor, ConversationContext, SyncProcessorOptions,
};
use pipecat_assemblyai::{AssemblyAiClient, AssemblyAiConfig, AssemblyAiSttProcessor};
use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(4), worker.recv())
        .await
        .expect("pipeline progress")
        .expect("pipeline output")
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let barrier = Frame::new(FrameData::Flush(FlushFrame {
        returning: false,
        origin: Some("review-feedback-barrier".into()),
    }));
    let id = barrier.id;
    worker
        .sender()
        .send(barrier, Direction::Downstream)
        .await
        .unwrap();
    let mut observed = vec![];
    loop {
        let frame = next(worker).await;
        if frame.frame.id == id {
            return observed;
        }
        observed.push(frame);
    }
}
async fn ready(worker: &mut PipelineWorker) {
    loop {
        if matches!(next(worker).await.frame.data, FrameData::Start(_)) {
            break;
        }
    }
}
async fn finish(worker: &mut PipelineWorker) {
    worker.end().await.unwrap();
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(4), worker.recv())
        .await
        .unwrap()
    {
        assert!(!matches!(frame.frame.data, FrameData::Error(ref error) if error.fatal));
    }
    worker.join().await.unwrap();
}
async fn complete(worker: &mut PipelineWorker, text: &str) -> Vec<DirectedFrame> {
    send(worker, FrameData::LlmResponseStart).await;
    send(worker, FrameData::LlmText(TextFrame::llm(text))).await;
    send(worker, FrameData::LlmResponseEnd).await;
    let mut observed = vec![];
    loop {
        let frame = next(worker).await;
        let upstream = frame.direction == Direction::Upstream
            && matches!(frame.frame.data, FrameData::LlmContextAssistantTurn(_));
        observed.push(frame);
        if upstream {
            return observed;
        }
    }
}
fn paired() -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default())
}
struct TranscriptTap(mpsc::Sender<String>);
#[async_trait]
impl FrameProcessor for TranscriptTap {
    fn name(&self) -> &str {
        "review-transcript-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Transcription(text) = &frame.data {
            self.0
                .try_send(text.text.clone())
                .map_err(|_| ProcessorError::new("review transcript channel unavailable"))?;
        }
        ctx.push(frame, direction).await
    }
}
async fn transcript(rx: &mut mpsc::Receiver<String>, expected: &str) {
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(4), rx.recv())
            .await
            .unwrap()
            .unwrap(),
        expected
    );
}

#[test]
fn review_turn_timestamp_is_first_start_and_commit_timestamp_is_finish() {
    let a = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    let before_start = Utc::now();
    a.process_frame(
        Frame::new(FrameData::LlmResponseStart),
        Direction::Downstream,
    )
    .unwrap();
    let after_start = Utc::now();
    std::thread::sleep(Duration::from_millis(30));
    let before_repeat = Utc::now();
    a.process_frame(
        Frame::new(FrameData::VisionFullResponseStart(Default::default())),
        Direction::Downstream,
    )
    .unwrap();
    a.process_frame(
        Frame::new(FrameData::LlmText(TextFrame::llm("committed"))),
        Direction::Downstream,
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(30));
    let before_commit = Utc::now();
    let output = a
        .process_frame(
            Frame::new(FrameData::VisionFullResponseEnd(Default::default())),
            Direction::Downstream,
        )
        .unwrap();
    let after_commit = Utc::now();
    let mut starts = vec![];
    let mut commits = vec![];
    for action in output.actions {
        if let AggregationAction::Push(frame) = action {
            match frame.frame.data {
                FrameData::LlmContextAssistantTurn(turn) => starts.push(
                    DateTime::parse_from_rfc3339(&turn.timestamp)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                FrameData::LlmContextAssistantTimestamp { timestamp } => commits.push(
                    DateTime::parse_from_rfc3339(&timestamp)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                _ => (),
            }
        }
    }
    assert_eq!(starts.len(), 2);
    assert_eq!(starts[0], starts[1]);
    assert_eq!(commits.len(), 1);
    let precision = chrono::Duration::milliseconds(1);
    assert!(starts[0] >= before_start - precision && starts[0] <= after_start);
    assert!(
        starts[0] < before_repeat,
        "repeated starts must not replace turn-start time"
    );
    assert!(commits[0] >= before_commit - precision && commits[0] <= after_commit);
    assert!(
        commits[0] > starts[0],
        "finish timestamp cannot alias turn-start time"
    );
}

#[tokio::test]
async fn review_ws_real_producer_keeps_identity_and_two_identical_turns() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = AssemblyAiConfig::new("fixture-key");
    config.endpoint = format!("ws://{}/v3/ws", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut updates = vec![];
        loop {
            let message = tokio::time::timeout(Duration::from_secs(4), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Message::Text(text) = message {
                let data: Value = serde_json::from_str(&text).unwrap();
                if data["type"] == "Terminate" {
                    socket.send(Message::Text(json!({"type":"Termination", "audio_duration_seconds":0.0,"session_duration_seconds":1.0}).to_string().into())).await.unwrap();
                    break;
                }
                updates.push(data);
            }
        }
        updates
    });
    let pair = paired();
    let stt = Arc::new(AssemblyAiSttProcessor::new(
        AssemblyAiClient::new(config).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![stt, pair.user(), pair.assistant()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    ready(&mut worker).await;
    for _ in 0..2 {
        let observed = complete(&mut worker, "● same answer ○").await;
        let turns: Vec<_> = observed
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
            .collect();
        assert_eq!(turns.len(), 2);
        assert_ne!(turns[0].frame.id, turns[1].frame.id);
        assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
        assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
        let timestamp = observed
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTimestamp { .. }))
            .unwrap();
        let down = observed
            .iter()
            .position(|f| {
                f.direction == Direction::Downstream
                    && matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))
            })
            .unwrap();
        assert!(timestamp < down);
    }
    for text in ["", "●◐○"] {
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::LlmText(TextFrame::llm(text))).await;
        send(&worker, FrameData::LlmResponseEnd).await;
        let observed = barrier(&mut worker).await;
        assert!(!observed
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))));
    }
    assert_eq!(
        pair.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("● same answer ○".into()))
    );
    finish(&mut worker).await;
    let updates = server.await.unwrap();
    assert_eq!(
        updates,
        vec![json!({"type":"UpdateConfiguration","agent_context":"same answer"}); 2]
    );
}

#[tokio::test]
async fn review_ws_disabled_carryover_and_non_u3_retain_consumer_policy() {
    for legacy in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = AssemblyAiConfig::new("fixture-key");
        config.endpoint = format!("ws://{}/v3/ws", listener.local_addr().unwrap());
        if legacy {
            config.settings.model = "universal-streaming-english".into();
        } else {
            config.settings.previous_context_n_turns = Some(0);
        }
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let message = tokio::time::timeout(Duration::from_secs(4), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(message.to_text().unwrap()).unwrap(),
                json!({"type":"Terminate"})
            );
            socket.send(Message::Text(json!({"type":"Termination", "audio_duration_seconds":0.0,"session_duration_seconds":1.0}).to_string().into())).await.unwrap();
        });
        let pair = paired();
        let stt = Arc::new(AssemblyAiSttProcessor::new(
            AssemblyAiClient::new(config).unwrap(),
        ));
        let mut worker = Pipeline::new(vec![stt, pair.user(), pair.assistant()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        ready(&mut worker).await;
        complete(&mut worker, "committed, but not a provider update").await;
        finish(&mut worker).await;
        server.await.unwrap();
    }
}

async fn feedback_socket() -> (AssemblyAiConfig, tokio::task::JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = AssemblyAiConfig::new("fixture-key");
    config.endpoint = format!("ws://{}/v3/ws", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut updates = vec![];
        loop {
            let message = tokio::time::timeout(Duration::from_secs(4), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Message::Text(text) = message {
                let data: Value = serde_json::from_str(&text).unwrap();
                if data["type"] == "Terminate" {
                    socket.send(Message::Text(json!({"type":"Termination", "audio_duration_seconds":0.0,"session_duration_seconds":1.0}).to_string().into())).await.unwrap();
                    return updates;
                }
                updates.push(data);
            }
        }
    });
    (config, server)
}

#[tokio::test]
async fn review_ws_paired_typed_and_vad_interruptions_deliver_one_partial_update() {
    for typed in [false, true] {
        let (config, server) = feedback_socket().await;
        let pair = paired();
        let stt = Arc::new(AssemblyAiSttProcessor::new(
            AssemblyAiClient::new(config).unwrap(),
        ));
        let mut worker = Pipeline::new(vec![stt, pair.user(), pair.assistant()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        ready(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::LlmText(TextFrame::llm("● partial accepted words")),
        )
        .await;
        assert!(!barrier(&mut worker)
            .await
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))));
        let trigger = if typed {
            FrameData::InputText(TextFrame::new("next question"))
        } else {
            FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.2,
                timestamp: 0.0,
            })
        };
        send(&worker, trigger).await;
        let mut turns = vec![];
        while turns.len() != 2 {
            let frame = next(&mut worker).await;
            if matches!(frame.frame.data, FrameData::LlmContextAssistantTurn(_)) {
                turns.push(frame);
            }
        }
        assert_ne!(turns[0].direction, turns[1].direction);
        assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
        send(&worker, FrameData::LlmResponseEnd).await;
        assert!(!barrier(&mut worker)
            .await
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))));
        assert_eq!(
            pair.context().snapshot().unwrap().messages[0].content,
            Some(Content::Text("● partial accepted words".into()))
        );
        finish(&mut worker).await;
        assert_eq!(
            server.await.unwrap(),
            vec![json!({"type":"UpdateConfiguration","agent_context":"partial accepted words"})]
        );
    }
}

struct ReceiptBridge {
    assistant: Arc<LLMAssistantAggregator>,
    texts: Mutex<Vec<AggregatedTextFrame>>,
}
#[async_trait]
impl FrameProcessor for ReceiptBridge {
    fn name(&self) -> &str {
        "review-confirmed-receipt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut receipt = None;
        match &frame.data {
            FrameData::TtsStarted(start) => self
                .assistant
                .begin_playback(start.context_id.as_deref().unwrap())?,
            FrameData::TtsText(text) => self.texts.lock().unwrap().push(text.clone()),
            FrameData::Flush(flush) if flush.origin.as_deref() == Some("review-receipt") => {
                let parts = std::mem::take(&mut *self.texts.lock().unwrap());
                receipt = Some(
                    self.assistant
                        .confirm_playback("verified-context", &parts)?,
                );
            }
            _ => (),
        }
        if let Some(output) = receipt {
            output.emit(ctx).await?;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_ws_generated_text_waits_for_confirmed_receipt_and_duplicate_receipt_is_empty() {
    let (config, server) = feedback_socket().await;
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let stt = Arc::new(AssemblyAiSttProcessor::new(
        AssemblyAiClient::new(config).unwrap(),
    ));
    let bridge = Arc::new(ReceiptBridge {
        assistant: pair.assistant(),
        texts: Mutex::new(vec![]),
    });
    let mut worker = Pipeline::new(vec![stt, pair.user(), bridge, pair.assistant()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    ready(&mut worker).await;
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("unverified generated words")),
        FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some("verified-context".into()),
            append_to_context: true,
        }),
        FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("confirmed receipt words"),
            context_id: Some("verified-context".into()),
            aggregated_by: "sentence".into(),
            raw_text: None,
            will_be_spoken: true,
        }),
        FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some("verified-context".into()),
        }),
        FrameData::LlmResponseEnd,
    ] {
        send(&worker, data).await;
    }
    assert!(!barrier(&mut worker)
        .await
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))));
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    send(
        &worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("review-receipt".into()),
        }),
    )
    .await;
    let mut turns = vec![];
    while turns.len() != 2 {
        let frame = next(&mut worker).await;
        if matches!(frame.frame.data, FrameData::LlmContextAssistantTurn(_)) {
            turns.push(frame);
        }
    }
    assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
    assert_eq!(
        pair.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("confirmed receipt words".into()))
    );
    send(
        &worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("review-receipt".into()),
        }),
    )
    .await;
    assert!(!barrier(&mut worker)
        .await
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))));
    finish(&mut worker).await;
    assert_eq!(
        server.await.unwrap(),
        vec![json!({"type":"UpdateConfiguration","agent_context":"confirmed receipt words"})]
    );
}

async fn http_fixture(
    replies: usize,
) -> (String, mpsc::Receiver<Value>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel(4);
    let task = tokio::spawn(async move {
        for n in 0..replies {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(4), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = vec![];
            let (split, length) = loop {
                let mut block = [0; 4096];
                let count = socket.read(&mut block).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
                assert!(bytes.len() < 128 * 1024);
                if let Some(split) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = std::str::from_utf8(&bytes[..split]).unwrap();
                    assert!(head.starts_with("POST /v1/transcribe "));
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (split + 4, length);
                }
            };
            while bytes.len() < split + length {
                let mut block = [0; 4096];
                let count = socket.read(&mut block).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
            }
            let body = &bytes[split..split + length];
            let header = b"name=\"config\"";
            let offset = body
                .windows(header.len())
                .position(|w| w == header)
                .unwrap();
            let tail = &body[offset..];
            let start = tail.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let tail = &tail[start..];
            let end = tail.windows(4).position(|w| w == b"\r\n--").unwrap();
            tx.send(serde_json::from_slice(&tail[..end]).unwrap())
                .await
                .unwrap();
            let body = json!({"text":format!("user {n}"),"words":[]}).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        }
    });
    (endpoint, rx, task)
}
async fn utterance(worker: &mut PipelineWorker) {
    let vad = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    // Use upstream VAD to exercise the actual STT control route without opening
    // a second user strategy turn in this consumer-focused fixture.
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStartedSpeaking(vad)),
            Direction::Upstream,
        )
        .await
        .unwrap();
    loop {
        if matches!(
            next(worker).await.frame.data,
            FrameData::VadUserStartedSpeaking(_)
        ) {
            break;
        }
    }
    send(
        worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![0; 3200], 16_000, 1).unwrap()),
    )
    .await;
    barrier(worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(vad)),
            Direction::Upstream,
        )
        .await
        .unwrap();
    // The user half consumes transcriptions, so a downstream barrier cannot
    // stand in for completed HTTP inference. Wait on the HTTP request outside.
}

#[tokio::test]
async fn review_sync_real_feedback_enters_next_chronological_multipart_context() {
    let (endpoint, mut requests, server) = http_fixture(2).await;
    let mut config = AssemblyAiSyncConfig::new("fixture-key");
    config.base_url = endpoint;
    let stt = Arc::new(
        AssemblyAiSyncProcessor::new(
            config,
            SyncProcessorOptions {
                enable_prewarming: false,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let pair = paired();
    let (tx, mut transcripts) = mpsc::channel(4);
    let mut worker = Pipeline::new(vec![stt, Arc::new(TranscriptTap(tx)), pair.assistant()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    ready(&mut worker).await;
    utterance(&mut worker).await;
    let first = tokio::time::timeout(Duration::from_secs(4), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(first.get("conversation_context").is_none());
    transcript(&mut transcripts, "user 0").await;
    complete(&mut worker, "● confirmed answer ○").await;
    utterance(&mut worker).await;
    let second = tokio::time::timeout(Duration::from_secs(4), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second["conversation_context"],
        json!(["user 0", "confirmed answer"])
    );
    transcript(&mut transcripts, "user 1").await;
    finish(&mut worker).await;
    server.await.unwrap();
}

#[tokio::test]
async fn review_sync_manual_context_and_disabled_budget_override_real_feedback() {
    for manual in [false, true] {
        let (endpoint, mut requests, server) = http_fixture(1).await;
        let mut config = AssemblyAiSyncConfig::new("fixture-key");
        config.base_url = endpoint;
        if manual {
            config.settings.conversation_context =
                Some(ConversationContext::Text("host-owned context".into()));
        } else {
            config.max_context_turns = 0;
        }
        let stt = Arc::new(
            AssemblyAiSyncProcessor::new(
                config,
                SyncProcessorOptions {
                    enable_prewarming: false,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let pair = paired();
        let (tx, mut transcripts) = mpsc::channel(4);
        let mut worker = Pipeline::new(vec![stt, Arc::new(TranscriptTap(tx)), pair.assistant()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        ready(&mut worker).await;
        complete(&mut worker, "new committed assistant reply").await;
        utterance(&mut worker).await;
        let request = tokio::time::timeout(Duration::from_secs(4), requests.recv())
            .await
            .unwrap()
            .unwrap();
        if manual {
            assert_eq!(request["conversation_context"], "host-owned context");
        } else {
            assert!(request.get("conversation_context").is_none());
        }
        transcript(&mut transcripts, "user 0").await;
        finish(&mut worker).await;
        server.await.unwrap();
    }
}
