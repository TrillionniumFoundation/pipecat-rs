//! Real loopback HTTP/WS exercises Cartesia's prepared pronunciation input.
//! PCM, timestamps, and playback receipts are synthetic fixtures. These tests
//! prove transport/provenance/history behavior, never real pronunciation quality.
#![cfg(feature = "assistant-history")]

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::*,
    services::{Content, Context, Role},
    speech::{cartesia::*, HttpTtsBackend, HttpTtsProcessor, SpeechConfig},
    streaming_speech::{CartesiaClient, CartesiaConfig, CartesiaTtsProcessor},
    text::{CartesiaPronunciationTransform, PronunciationLimits},
    turns::{AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::Instant,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const TEXT: &str = "Ada <<literal>> Ada tail.";
const ORIGINAL: &str = "**Ada <<literal>> Ada tail.**";
const SYNTHESIS: &str = "<<eɪ|d|ə>> <<literal>> <<eɪ|d|ə>> tail.";
const RATE: u32 = 8000;
const DEADLINE: Duration = Duration::from_secs(5);

fn prepared() -> TtsTextInputFrame {
    let transform = CartesiaPronunciationTransform::new(
        vec![("Ada".into(), "eɪdə".into())],
        PronunciationLimits::default(),
    )
    .unwrap();
    let mut input = TtsTextInputFrame::new(TEXT);
    input.original_text = ORIGINAL.into();
    transform.apply_to_input(&mut input).unwrap();
    assert_eq!(input.synthesis_text, SYNTHESIS);
    assert_eq!(input.display_text, TEXT);
    assert_eq!(input.original_text, ORIGINAL);
    input
}

struct Clock(Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        9_000_000_000 + self.0.elapsed().as_nanos() as u64
    }
}

async fn permit(gate: &Semaphore) -> Result<(), String> {
    gate.acquire()
        .await
        .map_err(|error| error.to_string())?
        .forget();
    Ok(())
}

struct ActiveWrite<'a>(&'a AtomicUsize);
impl Drop for ActiveWrite<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Playback {
    discard_last: bool,
    entered: Semaphore,
    release: Semaphore,
    returned: Semaphore,
    starts: Mutex<Vec<(PcmFormat, Vec<Option<String>>)>>,
    submitted: Mutex<Vec<Frame>>,
    receipts: Mutex<Vec<PlaybackReceipt>>,
    active: AtomicUsize,
    stops: AtomicUsize,
}
#[async_trait]
impl OutputBackend for Playback {
    async fn start(
        &self,
        format: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        self.starts
            .lock()
            .unwrap()
            .push((format, destinations.to_vec()));
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let index = {
            let mut submitted = self.submitted.lock().unwrap();
            let index = submitted.len();
            submitted.push(frame.clone());
            index
        };
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveWrite(&self.active);
        self.entered.add_permits(1);
        permit(&self.release).await.map_err(ProcessorError::new)?;
        let receipt = if self.discard_last && index == 2 {
            PlaybackReceipt::Discarded
        } else {
            PlaybackReceipt::Played
        };
        self.receipts.lock().unwrap().push(receipt);
        self.returned.add_permits(1);
        Ok(receipt)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Record actual processor frames and hold the original successful source end.
/// No TtsText or replacement completion frame is manufactured by the test.
struct TtsProbe {
    frames: Mutex<Vec<Frame>>,
    end_ready: Semaphore,
    release_end: Semaphore,
}
#[async_trait]
impl FrameProcessor for TtsProbe {
    fn name(&self) -> &str {
        "Cartesia-native-frame-proof"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
            if matches!(frame.data, FrameData::LlmResponseEnd) {
                self.end_ready.add_permits(1);
                permit(&self.release_end)
                    .await
                    .map_err(ProcessorError::new)?;
            }
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Default)]
struct AfterOutput(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for AfterOutput {
    fn name(&self) -> &str {
        "Cartesia-after-output-proof"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Debug)]
struct Request {
    head: String,
    body: Value,
}
async fn request(socket: &mut TcpStream) -> Result<Request, String> {
    let mut received = Vec::new();
    let header_end = loop {
        if let Some(index) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
        if received.len() >= 16 * 1024 {
            return Err("loopback request headers exceeded bound".into());
        }
        let mut chunk = [0; 1024];
        let count = socket
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("EOF before loopback request headers".into());
        }
        received.extend_from_slice(&chunk[..count]);
    };
    let head =
        String::from_utf8(received[..header_end].to_vec()).map_err(|error| error.to_string())?;
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .ok_or("missing loopback Content-Length")?;
    if length > 16 * 1024 {
        return Err("loopback request body exceeded bound".into());
    }
    while received.len() < header_end + length {
        let mut chunk = [0; 1024];
        let count = socket
            .read(&mut chunk)
            .await
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("EOF before loopback request body".into());
        }
        received.extend_from_slice(&chunk[..count]);
    }
    let body = serde_json::from_slice(&received[header_end..header_end + length])
        .map_err(|error| error.to_string())?;
    Ok(Request { head, body })
}

async fn finish(worker: &mut PipelineWorker, success: bool) -> Result<(), String> {
    let terminal = if success {
        worker.end().await
    } else {
        worker.cancel("Cartesia fixture cleanup").await
    }
    .map_err(|e| e.to_string());
    let report = match tokio::time::timeout(DEADLINE, worker.join()).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            let _ = worker.cancel("Cartesia join deadline").await;
            let joined = worker.join().await;
            return Err(format!(
                "pipeline timed out; cancelled and joined: {joined:?}"
            ));
        }
    };
    terminal?;
    if !report.errors.is_empty() {
        return Err(format!("{:?}", report.errors));
    }
    Ok(())
}

async fn send(worker: &PipelineWorker, data: FrameData) -> Result<(), String> {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn http_pronunciation_preserves_channels_dictionary_pcm_and_skip() {
    let input = prepared();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("synthetic");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = DEADLINE;
    let mut request_template = CartesiaSynthesisRequest::new("", "voice");
    request_template.sample_rate = RATE;
    request_template.pronunciation_dict_id = Some("separate-dictionary".into());
    let tts = HttpTtsProcessor::new(
        HttpTtsBackend::Cartesia {
            client: CartesiaHttpClient::new(config).unwrap(),
            request: request_template,
        },
        8,
    )
    .unwrap();
    let probe = Arc::new(AfterOutput::default());
    let mut worker = Pipeline::new(vec![Arc::new(tts), probe.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: RATE,
                ..Default::default()
            },
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let captured = request(&mut socket).await?;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\n\x01\x00\x02\x00").await.map_err(|e| e.to_string())?;
        socket.shutdown().await.map_err(|e| e.to_string())?;
        Ok::<_, String>((captured, listener))
    });
    let mut rejected = TtsTextInputFrame::new(TEXT);
    rejected.original_text = ORIGINAL.into();
    let before_rejection = rejected.clone();
    let bounded = CartesiaPronunciationTransform::new(
        vec![("Ada".into(), "eɪdə".into())],
        PronunciationLimits {
            max_output_bytes: 16,
            ..Default::default()
        },
    )
    .unwrap();
    let rejection = bounded.apply_to_input(&mut rejected);
    let scenario = tokio::time::timeout(DEADLINE, async {
        send(&worker, FrameData::LlmResponseStart).await?;
        // The caller admits only successfully prepared inputs. A regression in
        // limit checking would make an extra real request or fail this pipeline.
        if rejection.is_ok() {
            send(&worker, FrameData::TtsTextInput(rejected.clone())).await?;
        }
        let mut skip = input.clone();
        skip.skip_tts = true;
        send(&worker, FrameData::TtsTextInput(skip)).await?;
        send(&worker, FrameData::TtsTextInput(input)).await?;
        send(&worker, FrameData::LlmResponseEnd).await?;
        loop {
            let frame = worker.recv().await.ok_or("pipeline output closed")?;
            if matches!(frame.frame.data, FrameData::LlmResponseEnd) {
                break;
            }
        }
        Ok::<_, String>(())
    })
    .await
    .map_err(|_| "HTTP scenario timed out".to_owned())
    .and_then(|r| r);
    let joined = finish(&mut worker, scenario.is_ok()).await;
    let served = match tokio::time::timeout(DEADLINE, &mut server).await {
        Ok(r) => r.map_err(|e| e.to_string()).and_then(|r| r),
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("HTTP server timed out and joined".into())
        }
    };
    // Join all owners before behavioral assertions, including failure paths.
    assert!(
        scenario.is_ok(),
        "scenario={scenario:?}; joined={joined:?}; served={served:?}"
    );
    joined.unwrap();
    assert_eq!(
        rejection,
        Err(pipecat::text::PronunciationError::OutputTooLong)
    );
    assert_eq!(rejected, before_rejection);
    let (captured, listener) = served.unwrap();
    let extra_request = tokio::time::timeout(Duration::from_millis(50), listener.accept()).await;
    drop(listener);
    assert!(
        extra_request.is_err(),
        "skip_tts must not open another request"
    );
    assert!(captured.head.starts_with("POST /tts/bytes HTTP/1.1\r\n"));
    assert_eq!(captured.body["transcript"], SYNTHESIS);
    assert_eq!(
        captured.body["pronunciation_dict_id"],
        "separate-dictionary"
    );
    assert_eq!(captured.body["output_format"]["sample_rate"], RATE);
    let frames = probe.0.lock().unwrap();
    let aggregates: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::AggregatedText(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(aggregates.len(), 2);
    for aggregate in &aggregates {
        assert_eq!(aggregate.text.text, TEXT);
        assert_eq!(aggregate.raw_text.as_deref(), Some(ORIGINAL));
    }
    assert!(!aggregates[0].will_be_spoken);
    assert!(aggregates[1].will_be_spoken);
    let pcm: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAudio(a) => Some(a.audio.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(pcm, [1, 0, 2, 0]);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsText(_))));
    let diagnostic = frames
        .iter()
        .find_map(|f| match &f.data {
            FrameData::TtsAlignmentDiagnostic(d) => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(diagnostic.original_text.as_deref(), Some(ORIGINAL));
    assert_eq!(
        diagnostic.source_span,
        Some(TtsSourceSpan {
            start: 0,
            end: ORIGINAL.chars().count()
        })
    );
    assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
}

async fn history_case(discard: bool) {
    let input = prepared();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("synthetic", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    config.sample_rate = RATE;
    config.pronunciation_dict_id = Some("separate-dictionary".into());
    let tts = Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ));
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
    )
    .unwrap();
    let playback = Arc::new(Playback {
        discard_last: discard,
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        returned: Semaphore::new(0),
        starts: Mutex::new(vec![]),
        submitted: Mutex::new(vec![]),
        receipts: Mutex::new(vec![]),
        active: AtomicUsize::new(0),
        stops: AtomicUsize::new(0),
    });
    let probe = Arc::new(TtsProbe {
        frames: Mutex::new(vec![]),
        end_ready: Semaphore::new(0),
        release_end: Semaphore::new(0),
    });
    let output = Arc::new(
        OutputTransport::new(
            "Cartesia-controlled-output",
            OutputTransportConfig {
                sample_rate: Some(RATE),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: DEADLINE,
                ..Default::default()
            },
            playback.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![
        Arc::new(history.source_boundary("before-Cartesia").unwrap()),
        tts,
        probe.clone(),
        output,
        assistant,
    ])
    .with_observer_clock(Arc::new(Clock(Instant::now())))
    .start(PipelineConfig {
        shutdown_timeout: Duration::from_secs(1),
        start: StartFrame {
            audio_out_sample_rate: RATE,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let pcm = vec![0u8; (RATE * 30 / 1000 * 2) as usize];
    let expected_pcm = pcm.clone();
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let mut socket = accept_async(socket).await.map_err(|e| e.to_string())?;
        let first = socket
            .next()
            .await
            .ok_or("missing synthesis")?
            .map_err(|e| e.to_string())?;
        let request: Value = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let last = socket
            .next()
            .await
            .ok_or("missing flush")?
            .map_err(|e| e.to_string())?;
        let flush: Value = serde_json::from_str(last.to_text().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        // A deliberately synthetic single report covers the whole synthesis
        // range. No assumption about live Cartesia phoneme timestamp spelling.
        for event in [
            json!({"type":"timestamps", "context_id":request["context_id"], "word_timestamps":{"words":[SYNTHESIS],"start":[0.0],"end":[0.03]}}),
            json!({"type":"chunk", "context_id":request["context_id"], "data":STANDARD.encode(pcm)}),
            json!({"type":"done", "context_id":request["context_id"]}),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .map_err(|e| e.to_string())?;
        }
        // Native context completion closes its websocket; acknowledge and drop.
        while let Some(message) = socket.next().await {
            if let Message::Close(_) = message.map_err(|e| e.to_string())? {
                break;
            }
        }
        let _ = socket.close(None).await;
        Ok::<_, String>((request, flush))
    });
    let scenario = tokio::time::timeout(DEADLINE, async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
        send(&worker, FrameData::LlmResponseStart).await?;
        send(&worker, FrameData::TtsTextInput(input)).await?;
        send(&worker, FrameData::LlmResponseEnd).await?;
        permit(&playback.entered).await?;
        permit(&probe.end_ready).await?;
        let before_receipts = shared.snapshot().map_err(|e| e.to_string())?.messages.len();
        playback.release.add_permits(3);
        for _ in 0..3 {
            permit(&playback.returned).await?;
        }
        let before_source_end = shared.snapshot().map_err(|e| e.to_string())?.messages.len();
        probe.release_end.add_permits(1);
        while history.status().committed_contexts + history.status().withheld_contexts == 0 {
            tokio::task::yield_now().await;
        }
        Ok::<_, String>((before_receipts, before_source_end))
    })
    .await
    .map_err(|_| "WS history scenario timed out".to_owned())
    .and_then(|r| r);
    if scenario.is_err() {
        history.invalidate();
    }
    probe.release_end.add_permits(1);
    playback.release.add_permits(16);
    let joined = finish(&mut worker, scenario.is_ok()).await;
    let served = match tokio::time::timeout(DEADLINE, &mut server).await {
        Ok(r) => r.map_err(|e| e.to_string()).and_then(|r| r),
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("WS server timed out and joined".into())
        }
    };
    let observed = scenario.unwrap();
    joined.unwrap();
    let (request, flush) = served.unwrap();
    assert_eq!(request["transcript"], SYNTHESIS);
    assert_eq!(request["pronunciation_dict_id"], "separate-dictionary");
    assert_eq!(flush["continue"], false);
    assert_eq!(
        observed,
        (0, 0),
        "history requires both receipts and source end"
    );
    assert_eq!(playback.active.load(Ordering::SeqCst), 0);
    assert_eq!(playback.stops.load(Ordering::SeqCst), 1);
    let frames = probe.frames.lock().unwrap();
    let anchor = frames
        .iter()
        .find_map(|f| match &f.data {
            FrameData::AggregatedText(t) => Some((f.id, t)),
            _ => None,
        })
        .unwrap();
    assert_eq!(anchor.1.text.text, TEXT);
    assert_eq!(anchor.1.raw_text.as_deref(), Some(ORIGINAL));
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => Some((f, t)),
            _ => None,
        })
        .collect();
    assert_eq!(words.len(), 1);
    assert_eq!(words[0].1.raw_text.as_deref(), Some(ORIGINAL));
    assert_eq!(words[0].0.metadata["segment_id"], anchor.0);
    assert_eq!(words[0].0.metadata["alignment_evidence"], "provider_report");
    let actual_pcm: Vec<_> = playback
        .submitted
        .lock()
        .unwrap()
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAudio(a) | FrameData::OutputAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(actual_pcm, expected_pcm);
    let messages = shared.snapshot().unwrap().messages;
    if discard {
        assert!(messages.is_empty());
    } else {
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, Role::Assistant);
        assert!(matches!(&messages[0].content, Some(Content::Text(text)) if text == ORIGINAL));
    }
    assert_eq!(history.status().committed_contexts, u64::from(!discard));
    assert_eq!(history.status().withheld_contexts, u64::from(discard));
}

#[tokio::test]
async fn ws_pronunciation_original_range_enters_history_only_after_played_receipts() {
    history_case(false).await;
}
#[tokio::test]
async fn ws_pronunciation_discarded_pcm_withholds_original_range_history() {
    history_case(true).await;
}
