//! Two independent real Gemini WS compositions exercise the transcript copy:
//! A: RTVI -> standard assistant, for history, origin and cancellation.
//! B: RTVI -> real Cartesia TTS -> outlet, for skip-TTS and a synthesis control.
//! Cartesia buffers original TtsText behind synthesis, so B does not qualify
//! assistant-history ordering. Local peers do not establish provider acceptance
//! or playback. An interrupted pair may expose a prefix; it is not atomic.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use pipecat_rtvi::RtviObserver;
use pipecat_streaming_speech::{
    cartesia::{CartesiaClient, CartesiaConfig},
    CartesiaTtsProcessor,
};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Error as WsError, Message};

// The directly affected old graph: live mapping (28), audio pause (5), and
// its dedicated-STT mute regressions (4). No cumulative unrelated test graph.
#[path = "audio_pause.rs"]
mod audio_pause_regressions;

const WAIT: Duration = Duration::from_secs(5);
const REQUEST: &str = "transcript-request";
const NEXT: &str = "transcript-next-generation";
const CONTROL: &str = "CONTROL.";
const OLD: &str = "Old prefix.";
const FRESH: &str = "Fresh transcript. ";
const STT_DONE: &str = "stt-completed";
const INPUT_PCM: &[u8] = &[3, 0, 4, 0];
const GEMINI_PCM: &[u8] = &[1, 0, 2, 0];
const CONTROL_PCM: &[u8] = &[9, 0, 10, 0];

fn closed(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    ) || matches!(error, WsError::Io(error) if matches!(error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof))
}
async fn join<T>(mut task: JoinHandle<Result<T, String>>) -> Result<T, String> {
    match timeout(WAIT, &mut task).await {
        Ok(result) => result.map_err(|e| e.to_string()).and_then(|r| r),
        Err(error) => {
            task.abort();
            let _ = task.await;
            Err(format!("peer deadline; aborted and joined: {error}"))
        }
    }
}
fn transcript(text: &str) -> Value {
    json!({"serverContent":{"outputTranscription":{"text":text}}})
}
fn audio() -> Value {
    json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":STANDARD.encode(GEMINI_PCM)}}]}}})
}
fn control() -> Value {
    json!({"serverContent":{"modelTurn":{"parts":[{"text":CONTROL}]},"turnComplete":true}})
}
#[derive(Debug)]
struct GeminiTrace {
    setup: Value,
    wire: Vec<Value>,
    closed: bool,
}
struct GeminiPeer(JoinHandle<Result<GeminiTrace, String>>);
impl GeminiPeer {
    async fn open(
        mode: SessionMode,
        replies: Vec<Vec<Value>>,
    ) -> Result<(Self, GeminiClient), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = if mode == SessionMode::Conversation {
            GeminiConfig::new("transcript-fixture-key", "gemini-3.8-live")
        } else {
            GeminiConfig::transcription("transcript-fixture-key", "gemini-3.8-live")
        };
        config.endpoint = format!(
            "ws://{}/live",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.setup_timeout = Duration::from_secs(2);
        config.limits.write_timeout = Duration::from_secs(1);
        config.limits.finish_timeout = Duration::from_secs(1);
        // Validate before spawning a peer owner. No non-blocking tool opt-in.
        let client = GeminiClient::new(config).map_err(|e| e.to_string())?;
        let peer = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "Gemini accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
                .await
                .map_err(|_| "Gemini upgrade deadline")?
                .map_err(|e| e.to_string())?;
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "Gemini setup deadline")?
                .ok_or("no Gemini setup")?
                .map_err(|e| e.to_string())?;
            let setup = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let mut trace = GeminiTrace {
                setup,
                wire: vec![],
                closed: false,
            };
            timeout(
                WAIT,
                ws.send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                )),
            )
            .await
            .map_err(|_| "Gemini ready deadline")?
            .map_err(|e| e.to_string())?;
            let mut replies = replies.into_iter();
            loop {
                match timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "Gemini peer read deadline")?
                {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        let expected = if mode == SessionMode::Transcription {
                            value
                                == json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":STANDARD.encode(INPUT_PCM)}}})
                        } else {
                            matches!(
                                value["realtimeInput"]["text"].as_str(),
                                Some(REQUEST | NEXT)
                            )
                        };
                        if !expected {
                            return Err(format!("unexpected Gemini input: {value}"));
                        }
                        trace.wire.push(value);
                        for packet in replies.next().ok_or("too many Gemini requests")? {
                            timeout(WAIT, ws.send(Message::Text(packet.to_string().into())))
                                .await
                                .map_err(|_| "Gemini reply deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        timeout(WAIT, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "Gemini pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        trace.closed = true;
                        match timeout(WAIT, ws.flush())
                            .await
                            .map_err(|_| "Gemini close deadline")?
                        {
                            Ok(()) | Err(WsError::ConnectionClosed) => {}
                            Err(error) => return Err(error.to_string()),
                        }
                        break;
                    }
                    other => return Err(format!("expected Gemini WS Close: {other:?}")),
                }
            }
            if replies.next().is_some() {
                return Err("Gemini closed before all requests".into());
            }
            Ok(trace)
        });
        Ok((Self(peer), client))
    }
}

#[derive(Debug, Default)]
struct TtsTrace {
    connections: usize,
    wire: Vec<Value>,
    completions: usize,
    closed_connections: usize,
}
struct TtsPeer {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<TtsTrace, String>>,
}
impl TtsPeer {
    async fn open() -> Result<(Self, CartesiaClient), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = CartesiaConfig::new("transcript-tts-fixture", "fixture-voice");
        config.endpoint = format!(
            "ws://{}/tts",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.sample_rate = 24_000;
        config.add_timestamps = false;
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.write_timeout = Duration::from_secs(1);
        config.limits.response_timeout = Duration::from_secs(3);
        config.limits.close_timeout = Duration::from_secs(1);
        let client = CartesiaClient::new(config).map_err(|e| e.to_string())?;
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut trace = TtsTrace::default();
            loop {
                let accepted = tokio::select! {
                    biased;
                    _ = &mut stopped => return Ok(trace),
                    accepted = listener.accept() => accepted.map_err(|e| e.to_string())?,
                };
                trace.connections += 1;
                if trace.connections > 8 {
                    return Err("unexpected TTS connection count".into());
                }
                let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(accepted.0))
                    .await
                    .map_err(|_| "TTS upgrade deadline")?
                    .map_err(|e| e.to_string())?;
                let mut completed = false;
                loop {
                    // Once accepted, join only after the real consumer closes
                    // this connection. The stop signal only stops accepting.
                    let incoming = timeout(WAIT, ws.next())
                        .await
                        .map_err(|_| "TTS peer read deadline")?;
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value =
                                serde_json::from_str(&text).map_err(|e| e.to_string())?;
                            let done = value["continue"] == false;
                            let context = value["context_id"].clone();
                            trace.wire.push(value);
                            if trace.wire.len() > 32 {
                                return Err("unexpected TTS request count".into());
                            }
                            if done {
                                for event in [
                                    json!({"type":"chunk","context_id":context,"data":STANDARD.encode(CONTROL_PCM)}),
                                    json!({"type":"done","context_id":context}),
                                ] {
                                    timeout(WAIT, ws.send(Message::Text(event.to_string().into())))
                                        .await
                                        .map_err(|_| "TTS reply deadline")?
                                        .map_err(|e| e.to_string())?;
                                }
                                trace.completions += 1;
                                completed = true;
                            }
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            timeout(WAIT, ws.send(Message::Pong(bytes)))
                                .await
                                .map_err(|_| "TTS pong deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            match timeout(WAIT, ws.flush())
                                .await
                                .map_err(|_| "TTS close deadline")?
                            {
                                Ok(()) | Err(WsError::ConnectionClosed) => {}
                                // The Cartesia socket task sends Close then
                                // drops its socket after done. A reset while
                                // flushing our reply is valid only after done.
                                Err(error) if completed && closed(&error) => {}
                                Err(error) => return Err(error.to_string()),
                            }
                            trace.closed_connections += 1;
                            break;
                        }
                        None if completed => {
                            trace.closed_connections += 1;
                            break;
                        }
                        Some(Err(error)) if completed && closed(&error) => {
                            trace.closed_connections += 1;
                            break;
                        }
                        other => return Err(format!("unexpected TTS message: {other:?}")),
                    }
                }
            }
        });
        Ok((Self { stop, task }, client))
    }
    async fn finish(self) -> Result<TtsTrace, String> {
        let _ = self.stop.send(());
        join(self.task).await
    }
}

struct Receipt {
    stage: &'static str,
    frame: Frame,
    direction: Direction,
    fence: InvocationFence,
    current_after: bool,
}
struct Consumer {
    stage: &'static str,
    inner: Arc<dyn FrameProcessor>,
    receipt: mpsc::Sender<Receipt>,
}
#[async_trait]
impl FrameProcessor for Consumer {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let original = frame.clone();
        let fence = ctx.invocation_fence()?;
        self.inner.process(frame, direction, ctx).await?;
        let current_after = fence.is_current();
        self.receipt
            .try_send(Receipt {
                stage: self.stage,
                frame: original,
                direction,
                fence,
                current_after,
            })
            .map_err(|_| ProcessorError::fatal("test receipt capacity"))?;
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

#[derive(Default)]
struct PrefixGate {
    enabled: bool,
    entered: AtomicBool,
    entered_signal: Notify,
    held_frame: Mutex<Option<Frame>>,
    dropped: Arc<AtomicBool>,
    fence: Mutex<Option<InvocationFence>>,
    release: Notify,
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for PrefixGate {
    fn name(&self) -> &str {
        "TranscriptPrefixGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.enabled && matches!(&frame.data, FrameData::LlmText(text) if text.text == OLD) {
            *self
                .fence
                .lock()
                .map_err(|_| ProcessorError::fatal("gate fence poisoned"))? =
                Some(ctx.invocation_fence()?);
            *self
                .held_frame
                .lock()
                .map_err(|_| ProcessorError::fatal("gate frame poisoned"))? = Some(frame.clone());
            let _drop = Dropped(self.dropped.clone());
            self.entered.store(true, Ordering::SeqCst);
            self.entered_signal.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Composition {
    Assistant,
    Tts,
}
impl Composition {
    fn stage(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::Tts => "tts",
        }
    }
    fn final_stage(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::Tts => "outlet",
        }
    }
}
struct Observed {
    composition: Composition,
    outlets: Vec<DirectedFrame>,
    receipts: Vec<Receipt>,
    history: LlmContextData,
    gemini: GeminiTrace,
    tts: Option<TtsTrace>,
    request_ids: Vec<u64>,
    retired_fence: Option<InvocationFence>,
    retired_prefix: Option<Frame>,
}
async fn pump(
    worker: &mut PipelineWorker,
    rx: &mut mpsc::Receiver<Receipt>,
    outlets: &mut Vec<DirectedFrame>,
    receipts: &mut Vec<Receipt>,
) -> Result<(), String> {
    tokio::select! {
        frame = worker.recv() => {
            let frame = frame.ok_or("pipeline outlet closed before witness")?;
            let error = match &frame.frame.data { FrameData::Error(error) => Some(error.error.clone()), _ => None };
            outlets.push(frame);
            if let Some(error) = error { return Err(format!("pipeline error: {error}")); }
        }
        receipt = rx.recv() => receipts.push(receipt.ok_or("consumer receipts closed")?),
    }
    Ok(())
}
fn llm_wire(outlets: &[DirectedFrame]) -> Vec<String> {
    outlets
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::OutputTransportMessageUrgent(value) if value["type"] == "bot-llm-text" => {
                value["data"]["text"].as_str().map(str::to_owned)
            }
            _ => None,
        })
        .collect()
}
async fn request(
    worker: &PipelineWorker,
    mode: SessionMode,
    text: &str,
    context: &str,
) -> Result<u64, String> {
    let mut frame = Frame::new(if mode == SessionMode::Transcription {
        FrameData::InputAudio(
            AudioRawFrame::new(INPUT_PCM.to_vec(), 16_000, 1).map_err(|e| e.to_string())?,
        )
    } else {
        FrameData::InputText(TextFrame::new(text))
    });
    frame.metadata.insert("context_id".into(), json!(context));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .map_err(|e| e.to_string())?;
    Ok(id)
}
async fn execute(
    composition: Composition,
    mode: SessionMode,
    replies: Vec<Vec<Value>>,
    interrupted: bool,
) -> Observed {
    let observer = Arc::new(RtviObserver::new(Default::default(), None).unwrap());
    let (tts_peer, tts_client) = if composition == Composition::Tts {
        let (peer, client) = TtsPeer::open().await.unwrap();
        (Some(peer), Some(client))
    } else {
        (None, None)
    };
    let (gemini_peer, gemini_client) = match GeminiPeer::open(mode, replies).await {
        Ok(value) => value,
        Err(error) => {
            let tts = finish_tts(tts_peer).await;
            panic!("Gemini configuration: {error}; TTS: {tts:?}");
        }
    };
    let shared = SharedContext::default();
    let gate = Arc::new(PrefixGate {
        enabled: interrupted,
        ..Default::default()
    });
    let (receipt, mut rx) = mpsc::channel(256);
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![
        Arc::new(GeminiLiveProcessor::new(gemini_client)),
        Arc::new(observer.tap("transcript-rtvi")),
        gate.clone(),
    ];
    match tts_client {
        Some(client) => {
            processors.push(Arc::new(Consumer {
                stage: "tts",
                inner: Arc::new(CartesiaTtsProcessor::new(client)),
                receipt: receipt.clone(),
            }));
            processors.push(Arc::new(Consumer {
                stage: "outlet",
                inner: Arc::new(IdentityProcessor),
                receipt,
            }));
        }
        None => processors.push(Arc::new(Consumer {
            stage: "assistant",
            inner: Arc::new(LLMAssistantAggregator::new(
                shared.clone(),
                Default::default(),
            )),
            receipt,
        })),
    }
    let pipeline = Pipeline::new(processors);
    let created = pipeline
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            start: StartFrame {
                audio_out_sample_rate: 24_000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let mut worker = match created {
        Ok(worker) => worker,
        Err(error) => {
            gate.release.notify_one();
            let gemini = join(gemini_peer.0).await;
            let tts = finish_tts(tts_peer).await;
            panic!("pipeline start: {error}; Gemini: {gemini:?}; TTS: {tts:?}");
        }
    };
    let mut outlets = vec![];
    let mut receipts = vec![];
    let mut request_ids = vec![];
    let mut retired_fence = None;
    let mut retired_prefix = None;
    let observation = timeout(WAIT, async {
        while !outlets.iter().any(|f: &DirectedFrame| f.frame.data.is_start()) {
            pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
        }
        request_ids.push(request(&worker, mode, REQUEST, "transcript-old").await?);
        if interrupted {
            while !gate.entered.load(Ordering::SeqCst) || !llm_wire(&outlets).iter().any(|text| text == OLD) {
                tokio::select! {
                    result = pump(&mut worker, &mut rx, &mut outlets, &mut receipts) => result?,
                    _ = gate.entered_signal.notified() => {},
                }
            }
            retired_prefix = gate.held_frame.lock().map_err(|_| "gate frame poisoned")?.clone();
            let fence = gate.fence.lock().map_err(|_| "gate fence poisoned")?.clone().ok_or("gate has no fence")?;
            if !fence.is_current() { return Err("old prefix was already stale before interruption".into()); }
            let interrupt = Frame::interruption();
            let interrupt_id = interrupt.id;
            worker.sender().send(interrupt, Direction::Downstream).await.map_err(|e| e.to_string())?;
            while !outlets.iter().any(|f| f.frame.id == interrupt_id && matches!(f.frame.data, FrameData::Interruption)) {
                pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
            }
            if fence.is_current() || !gate.dropped.load(Ordering::SeqCst) { return Err("interruption did not retire the held invocation".into()); }
            retired_fence = Some(fence);
            // The suffix may already have been queued. The observed cutoff
            // proves retirement at this consumer boundary, not atomic emission.
            request_ids.push(request(&worker, mode, NEXT, "transcript-new").await?);
        }
        if mode == SessionMode::Transcription {
            while !receipts.iter().any(|r| r.stage == composition.final_stage() && matches!(&r.frame.data, FrameData::Transcription(t) if t.text == STT_DONE && t.finalized)) {
                pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
            }
            if composition == Composition::Tts {
                // STT produces no model text; exercise the same real consumer
                // with an independent positive control after the normal-lane
                // final-transcription receipt.
                worker.sender().send(Frame::new(FrameData::TtsSpeak(TtsSpeakFrame { text: CONTROL.into(), append_to_context: false })), Direction::Downstream).await.map_err(|e| e.to_string())?;
                while !receipts.iter().any(|r| r.stage == "outlet" && matches!(r.frame.data, FrameData::TtsStopped(_))) {
                    pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
                }
            }
        } else {
            while !receipts.iter().any(|r| r.stage == composition.final_stage() && matches!(r.frame.data, FrameData::LlmResponseEnd)) {
                pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
            }
        }
        if composition == Composition::Assistant && mode == SessionMode::Conversation {
            // A process-return receipt alone is not the history outlet.
            // Wait for the standard assistant's actual two-way commit feedback.
            for direction in [Direction::Downstream, Direction::Upstream] {
                while !outlets.iter().any(|f| f.direction == direction && matches!(&f.frame.data, FrameData::LlmContextAssistantTurn(turn) if turn.text.ends_with(CONTROL))) {
                    pump(&mut worker, &mut rx, &mut outlets, &mut receipts).await?;
                }
            }
        }
        Ok::<_, String>(())
    }).await.map_err(|_| "pipeline observation deadline".to_owned()).and_then(|r| r);
    let normal = observation.is_ok();
    gate.release.notify_one();
    let terminal = timeout(WAIT, async {
        if normal {
            worker.end().await
        } else {
            worker.cancel("transcript observation failed").await
        }
    })
    .await;
    if !matches!(terminal, Ok(Ok(()))) {
        let _ = timeout(WAIT, worker.cancel("terminal deadline")).await;
    }
    let mut join_timeout = false;
    let joined = match timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            outlets.push(frame);
        }
        worker.join().await
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            join_timeout = true;
            let _ = timeout(WAIT, worker.cancel("join deadline")).await;
            worker.join().await
        }
    };
    while let Ok(receipt) = rx.try_recv() {
        receipts.push(receipt);
    }
    let history = shared.snapshot_rich();
    let gemini = join(gemini_peer.0).await;
    let tts = finish_tts(tts_peer).await;
    // All live owners have completed or have been aborted and actually joined.
    assert!(normal, "{observation:?}; Gemini: {gemini:?}; TTS: {tts:?}");
    assert!(matches!(terminal, Ok(Ok(()))), "{terminal:?}");
    assert!(!join_timeout);
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    let gemini = gemini.unwrap();
    assert!(gemini.closed);
    assert_eq!(gemini.setup["setup"]["model"], "models/gemini-3.8-live");
    let expected_requests = if mode == SessionMode::Transcription {
        assert_eq!(
            gemini.setup["setup"]["generationConfig"]["responseModalities"],
            json!(["TEXT"])
        );
        assert!(gemini.setup["setup"]
            .get("outputAudioTranscription")
            .is_none());
        vec![
            json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":STANDARD.encode(INPUT_PCM)}}}),
        ]
    } else {
        assert_eq!(
            gemini.setup["setup"]["generationConfig"]["responseModalities"],
            json!(["AUDIO"])
        );
        assert_eq!(gemini.setup["setup"]["outputAudioTranscription"], json!({}));
        (if interrupted {
            vec![REQUEST, NEXT]
        } else {
            vec![REQUEST]
        })
        .into_iter()
        .map(|text| json!({"realtimeInput":{"text":text}}))
        .collect()
    };
    assert_eq!(gemini.wire, expected_requests);
    Observed {
        composition,
        outlets,
        receipts,
        history: history.unwrap(),
        gemini,
        tts: tts.unwrap(),
        request_ids,
        retired_fence,
        retired_prefix,
    }
}

async fn finish_tts(peer: Option<TtsPeer>) -> Result<Option<TtsTrace>, String> {
    match peer {
        Some(peer) => peer.finish().await.map(Some),
        None => Ok(None),
    }
}
fn transcript_receipts<'a>(observed: &'a Observed, stage: &str) -> Vec<&'a Receipt> {
    observed
        .receipts
        .iter()
        .filter(|r| {
            r.stage == stage
                && r.frame.metadata.contains_key("gemini_generation")
                && match &r.frame.data {
                    FrameData::LlmText(text) => text.text != CONTROL,
                    FrameData::TtsText(_) => true,
                    _ => false,
                }
        })
        .collect()
}
fn check_pairs(
    observed: &Observed,
    expected: &[&str],
    generation: u64,
    source_id: u64,
    context: &str,
) {
    // These receipts are emitted only after the actual selected production
    // consumer returns. They preserve its input order, not asynchronous output
    // order from Cartesia's owned completion tasks.
    let stage = observed.composition.stage();
    let frames = transcript_receipts(observed, stage);
    assert_eq!(frames.len(), expected.len() * 2, "stage {stage}");
    for (pair, text) in frames.as_chunks::<2>().0.iter().zip(expected) {
        let FrameData::LlmText(copy) = &pair[0].frame.data else {
            panic!("copy must precede TTS text");
        };
        let FrameData::TtsText(original) = &pair[1].frame.data else {
            panic!("missing original TTS text");
        };
        assert_eq!(copy.text, *text);
        assert!(!copy.append_to_context);
        assert_eq!(copy.skip_tts, Some(true));
        assert!(copy.includes_inter_frame_spaces);
        assert_eq!(original.text.text, *text);
        assert!(original.text.append_to_context);
        assert_eq!(original.text.skip_tts, Some(true));
        assert!(original.text.includes_inter_frame_spaces);
        assert_eq!(original.aggregated_by, "word");
        assert!(original.will_be_spoken);
        assert!(original.raw_text.is_none());
        assert_eq!(original.context_id.as_deref(), Some(context));
        assert_ne!(pair[0].frame.id, pair[1].frame.id);
        for receipt in pair {
            assert_eq!(receipt.direction, Direction::Downstream);
            assert!(
                receipt.current_after,
                "consumer returned with a stale invocation"
            );
            assert_eq!(receipt.frame.metadata["source_frame_id"], source_id);
            assert_eq!(receipt.frame.metadata["context_id"], context);
            assert_eq!(receipt.frame.metadata["gemini_generation"], generation);
            assert!(!receipt.frame.metadata.contains_key("spoken"));
            assert!(
                !receipt.fence.is_current(),
                "joined runtime retained a current fence"
            );
        }
        assert_eq!(pair[0].frame.metadata, pair[1].frame.metadata);
    }
    if observed.composition == Composition::Tts {
        // Originals may leave Cartesia after response-end. Require every exact
        // skipped copy and original once at the actual outlet, without claiming
        // a production-history ordering through this different composition.
        for receipt in &frames {
            let outputs: Vec<_> = observed
                .outlets
                .iter()
                .filter(|f| f.frame.id == receipt.frame.id)
                .collect();
            assert_eq!(outputs.len(), 1, "lost or replayed transcript frame");
            assert_eq!(outputs[0].direction, Direction::Downstream);
            assert_eq!(outputs[0].frame, receipt.frame);
        }
        assert_eq!(transcript_receipts(observed, "outlet").len(), frames.len());
    }
}
fn check_tts_control(observed: &Observed, gemini_audio: usize) {
    assert_eq!(observed.composition, Composition::Tts);
    let tts = observed.tts.as_ref().expect("real TTS peer trace");
    assert_eq!(tts.connections, 1, "{tts:?}");
    assert_eq!(tts.completions, 1);
    assert_eq!(tts.closed_connections, 1);
    assert_eq!(tts.wire.len(), 2, "{:?}", tts.wire);
    assert_eq!(tts.wire[0]["transcript"], CONTROL);
    assert_eq!(tts.wire[0]["continue"], true);
    assert_eq!(tts.wire[1]["transcript"], "");
    assert_eq!(tts.wire[1]["continue"], false);
    assert_eq!(tts.wire[0]["context_id"], tts.wire[1]["context_id"]);
    assert!(tts.wire[0]["context_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert_eq!(tts.wire[0]["output_format"]["sample_rate"], 24_000);
    let audio: Vec<_> = observed
        .outlets
        .iter()
        .filter_map(|r| match &r.frame.data {
            FrameData::TtsAudio(audio) => Some(audio.audio.as_slice()),
            _ => None,
        })
        .collect();
    assert_eq!(
        audio.iter().filter(|bytes| **bytes == CONTROL_PCM).count(),
        1
    );
    assert_eq!(
        audio.iter().filter(|bytes| **bytes == GEMINI_PCM).count(),
        gemini_audio
    );
    assert_eq!(audio.len(), gemini_audio + 1);
    assert_eq!(observed.outlets.iter().filter(|f| matches!(&f.frame.data, FrameData::TtsStopped(stopped) if stopped.context_id.as_deref() == tts.wire[0]["context_id"].as_str())).count(), 1);
}
fn check_history(observed: &Observed, expected: &[&str]) {
    assert_eq!(observed.composition, Composition::Assistant);
    assert_eq!(history(observed), expected);
    for direction in [Direction::Downstream, Direction::Upstream] {
        let feedback: Vec<_> = observed
            .outlets
            .iter()
            .filter_map(|f| {
                if f.direction != direction {
                    return None;
                }
                match &f.frame.data {
                    FrameData::LlmContextAssistantTurn(turn) => Some(turn.text.as_str()),
                    _ => None,
                }
            })
            .collect();
        assert_eq!(
            feedback, expected,
            "standard assistant committed feedback {direction:?}"
        );
    }
}
fn history(observed: &Observed) -> Vec<String> {
    observed
        .history
        .messages
        .iter()
        .map(|message| {
            assert_eq!(message.role, ChatRole::Assistant);
            let MessageContent::Text(text) = &message.content else {
                panic!("unexpected non-text history");
            };
            text.clone()
        })
        .collect()
}

#[tokio::test]
async fn transcript_before_or_after_audio_reaches_rtvi_once_without_duplicate_history_or_tts() {
    for audio_first in [false, true] {
        for composition in [Composition::Assistant, Composition::Tts] {
            let texts = ["Hello. ", "Repeat. ", "Repeat. "];
            let mut packets = texts
                .iter()
                .map(|text| transcript(text))
                .collect::<Vec<_>>();
            if audio_first {
                packets.insert(0, audio());
            } else {
                packets.push(audio());
            }
            packets.push(control());
            let observed =
                execute(composition, SessionMode::Conversation, vec![packets], false).await;
            check_pairs(
                &observed,
                &texts,
                0,
                observed.request_ids[0],
                "transcript-old",
            );
            assert_eq!(
                llm_wire(&observed.outlets),
                vec!["Hello. ", "Repeat. ", "Repeat. ", CONTROL]
            );
            match composition {
                Composition::Assistant => {
                    check_history(&observed, &["Hello. Repeat. Repeat. CONTROL."])
                }
                Composition::Tts => check_tts_control(&observed, 1),
            }
        }
    }
}

#[tokio::test]
async fn missing_empty_and_whitespace_transcripts_keep_exact_conversation_and_stt_boundaries() {
    let omitted = vec![
        json!({"serverContent":{}}),
        json!({"serverContent":{"outputTranscription":null}}),
        json!({"serverContent":{"outputTranscription":{}}}),
        transcript(""),
        transcript("   "),
    ];
    for composition in [Composition::Assistant, Composition::Tts] {
        let mut conversation = omitted.clone();
        conversation.push(control());
        let observed = execute(
            composition,
            SessionMode::Conversation,
            vec![conversation],
            false,
        )
        .await;
        check_pairs(
            &observed,
            &["   "],
            0,
            observed.request_ids[0],
            "transcript-old",
        );
        assert_eq!(llm_wire(&observed.outlets), vec!["   ", CONTROL]);
        match composition {
            Composition::Assistant => check_history(&observed, &[CONTROL]),
            Composition::Tts => check_tts_control(&observed, 0),
        }

        let mut stt = omitted.clone();
        stt.push(transcript("conversation-only"));
        stt.push(
            json!({"serverContent":{"inputTranscription":{"text":STT_DONE},"turnComplete":true}}),
        );
        let observed = execute(composition, SessionMode::Transcription, vec![stt], false).await;
        assert!(transcript_receipts(&observed, composition.stage()).is_empty());
        assert!(llm_wire(&observed.outlets).is_empty());
        match composition {
            Composition::Assistant => check_history(&observed, &[]),
            Composition::Tts => check_tts_control(&observed, 0),
        }
    }
}

#[tokio::test]
async fn interrupted_pair_may_expose_rtvi_prefix_but_old_suffix_cannot_reach_consumers() {
    let stale =
        json!({"serverContent":{"outputTranscription":{"text":"stale tail"},"turnComplete":true}});
    let observed = execute(
        Composition::Assistant,
        SessionMode::Conversation,
        vec![
            vec![transcript(OLD)],
            vec![stale, transcript(FRESH), control()],
        ],
        true,
    )
    .await;
    assert!(observed
        .retired_fence
        .as_ref()
        .is_some_and(|fence| !fence.is_current()));
    assert_eq!(observed.gemini.wire.len(), 2);
    let prefix = observed
        .retired_prefix
        .as_ref()
        .expect("actually held prefix");
    let FrameData::LlmText(text) = &prefix.data else {
        panic!("gate held a different frame");
    };
    assert_eq!(text.text, OLD);
    assert!(!text.append_to_context);
    assert_eq!(text.skip_tts, Some(true));
    assert_eq!(prefix.metadata["source_frame_id"], observed.request_ids[0]);
    assert_eq!(prefix.metadata["context_id"], "transcript-old");
    assert_eq!(prefix.metadata["gemini_generation"], 0);
    assert!(!observed
        .receipts
        .iter()
        .any(|receipt| receipt.stage == "assistant" && receipt.frame.id == prefix.id));
    check_pairs(
        &observed,
        &[FRESH],
        1,
        observed.request_ids[1],
        "transcript-new",
    );
    assert_eq!(llm_wire(&observed.outlets), vec![OLD, FRESH, CONTROL]);
    check_history(&observed, &["Fresh transcript. CONTROL."]);
}
