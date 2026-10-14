//! Real loopback HTTP/SSE crosses the Speechify client, alignment processor,
//! native output, and assistant-history receipt fence. PCM and Played receipts
//! are controlled test data: no live Speechify account or audio device is used.
#![cfg(all(feature = "speechify", feature = "assistant-history"))]

use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat::{
    audio::{encode_pcm16_le, resample_pcm16, PcmFormat},
    core::*,
    media_transport::*,
    services::{Content, Context, Role},
    speech::{speechify::*, HttpTtsBackend, HttpTtsProcessor, SpeechConfig},
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

const TEXT: &str = "text-to-speech isn’t naïve.";
const RATE: u32 = 8000;
const DEADLINE: Duration = Duration::from_secs(5);

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
        "Speechify-native-frame-proof"
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
        "Speechify-after-output-proof"
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

async fn event(socket: &mut TcpStream, name: &str, data: Value) -> Result<(), String> {
    let body = format!("event: {name}\r\ndata: {data}\r\n\r\n");
    // HTTP chunk boundaries deliberately do not coincide with SSE records.
    for part in body.as_bytes().chunks(17) {
        socket
            .write_all(format!("{:x}\r\n", part.len()).as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        socket
            .write_all(part)
            .await
            .map_err(|error| error.to_string())?;
        socket
            .write_all(b"\r\n")
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn mark(start: usize, end: usize, value: &str, time: u32, end_time: u32) -> Value {
    json!({"type":"word", "start":start, "end":end, "value":value,
        "start_time":time, "end_time":end_time})
}

async fn serve(
    listener: TcpListener,
    finish: Arc<Semaphore>,
    pcm: Vec<u8>,
) -> Result<Request, String> {
    let (mut socket, _) = listener.accept().await.map_err(|error| error.to_string())?;
    let request = request(&mut socket).await?;
    let first_ten_ms = pcm.len() / 3;
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await.map_err(|error| error.to_string())?;
    event(
        &mut socket,
        "speech.chunk",
        json!({
            "audio": STANDARD.encode(&pcm[..first_ten_ms]),
            "speech_marks": [mark(0,4,"text",0,2), mark(4,5,"-",2,3),
                mark(5,7,"to",3,5), mark(7,8,"-",5,6)]
        }),
    )
    .await?;
    event(
        &mut socket,
        "speech.chunk",
        json!({
            "audio": STANDARD.encode(&pcm[first_ten_ms..]),
            "speech_marks": [mark(8,14,"speech",6,10), mark(15,18,"isn",10,15),
                mark(18,19,"'",15,16)]
        }),
    )
    .await?;
    permit(&finish).await?;
    event(&mut socket, "speech.done", json!({"audio_duration_ms":30})).await?;
    // Marks-only events can follow done. Values normalize Unicode; authoritative
    // code-point spans must restore the apostrophe and diaeresis from input.
    event(
        &mut socket,
        "speech.chunk",
        json!({"speech_marks":[
            mark(19,20,"t",16,20), mark(21,27,"naive.",20,27)
        ]}),
    )
    .await?;
    socket
        .write_all(b"0\r\n\r\n")
        .await
        .map_err(|error| error.to_string())?;
    socket.shutdown().await.map_err(|error| error.to_string())?;
    Ok(request)
}

fn text(shared: &SharedContext) -> Result<Vec<String>, String> {
    shared
        .snapshot()
        .map_err(|error| error.to_string())?
        .messages
        .into_iter()
        .map(|message| match (message.role, message.content) {
            (Role::Assistant, Some(Content::Text(text))) => Ok(text),
            _ => Err("unexpected non-assistant text in heard history".into()),
        })
        .collect()
}

#[derive(Debug)]
struct Snapshot {
    label: &'static str,
    text: Vec<String>,
    committed: u64,
    submissions: usize,
    receipts: usize,
}
fn snapshot(
    label: &'static str,
    shared: &SharedContext,
    history: &SpokenHistory,
    playback: &Playback,
) -> Result<Snapshot, String> {
    Ok(Snapshot {
        label,
        text: text(shared)?,
        committed: history.status().committed_contexts,
        submissions: playback.submitted.lock().unwrap().len(),
        receipts: playback.receipts.lock().unwrap().len(),
    })
}

async fn check_receipt_fence(discard_last: bool, output_rate: u32) {
    let source_rate = if output_rate == 32000 {
        24000
    } else {
        output_rate
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechify("synthetic");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = DEADLINE;
    let mut request = SpeechifySynthesisRequest::new("");
    request.sample_rate = output_rate;
    request.language = Some("en".into());
    request.loudness_normalization = Some(false);
    request.text_normalization = Some(true);
    let tts = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechify {
                client: SpeechifyHttpClient::new(config).unwrap(),
                request,
            },
            8,
        )
        .unwrap(),
    );
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
        discard_last,
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
    let after_output = Arc::new(AfterOutput::default());
    let output = Arc::new(
        OutputTransport::new(
            "Speechify-controlled-output",
            OutputTransportConfig {
                sample_rate: Some(output_rate),
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
    // Owners stay outside all timeout futures. On failure they are cancelled or
    // aborted, then joined before any behavioral assertion can panic.
    let mut worker = Pipeline::new(vec![
        Arc::new(history.source_boundary("source-before-Speechify").unwrap()),
        tts,
        probe.clone(),
        output,
        after_output.clone(),
        assistant,
    ])
    .with_observer_clock(Arc::new(Clock(Instant::now())))
    .start(PipelineConfig {
        shutdown_timeout: Duration::from_secs(1),
        start: StartFrame {
            audio_out_sample_rate: output_rate,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let pcm = encode_pcm16_le(
        &(0..source_rate * 30 / 1000)
            .map(|index| (index * 3) as i16 - 300)
            .collect::<Vec<_>>(),
    );
    let finish_provider = Arc::new(Semaphore::new(0));
    let mut server = tokio::spawn(serve(listener, finish_provider.clone(), pcm.clone()));
    let mut start = Frame::new(FrameData::LlmResponseStart);
    start
        .metadata
        .insert("test.original-start".into(), true.into());
    let start_id = start.id;
    let input = Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(TEXT)));
    let input_id = input.id;
    let mut end = Frame::new(FrameData::LlmResponseEnd);
    end.metadata.insert("test.original-end".into(), true.into());
    let end_id = end.id;
    let scenario = tokio::time::timeout(DEADLINE, async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
        for frame in [start, input, end] {
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .map_err(|error| error.to_string())?;
        }
        let mut observations = vec![];
        permit(&playback.entered).await?;
        observations.push(snapshot(
            "submitted, receipt pending",
            &shared,
            &history,
            &playback,
        )?);
        finish_provider.add_permits(1);
        permit(&probe.end_ready).await?;
        observations.push(snapshot(
            "provider done and EOF, receipt pending",
            &shared,
            &history,
            &playback,
        )?);
        playback.release.add_permits(2);
        permit(&playback.entered).await?;
        permit(&playback.entered).await?;
        observations.push(snapshot(
            "two Played, final receipt pending",
            &shared,
            &history,
            &playback,
        )?);
        playback.release.add_permits(1);
        for _ in 0..3 {
            permit(&playback.returned).await?;
        }
        observations.push(snapshot(
            "all receipts, original source end held",
            &shared,
            &history,
            &playback,
        )?);
        probe.release_end.add_permits(1);
        loop {
            let status = history.status();
            if status.committed_contexts + status.withheld_contexts > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, String>(observations)
    })
    .await
    .map_err(|_| "Speechify receipt-fence scenario timed out".to_owned())
    .and_then(|result| result);

    if scenario.is_err() {
        history.invalidate();
    }
    finish_provider.add_permits(1);
    probe.release_end.add_permits(1);
    playback.release.add_permits(16);
    let terminal = if scenario.is_ok() {
        worker.end().await
    } else {
        worker.cancel("Speechify receipt-fence cleanup").await
    };
    let joined = match tokio::time::timeout(DEADLINE, worker.join()).await {
        Ok(result) => result.map_err(|error| error.to_string()),
        Err(_) => {
            history.invalidate();
            let _ = worker.cancel("Speechify worker join deadline").await;
            // Core's shutdown timeout owns abort-and-join of background work.
            let result = worker.join().await;
            Err(format!(
                "worker join timed out; cancelled and joined: {result:?}"
            ))
        }
    };
    let served = match tokio::time::timeout(DEADLINE, &mut server).await {
        Ok(result) => result
            .map_err(|error| error.to_string())
            .and_then(|result| result),
        Err(_) => {
            server.abort();
            let result = server.await;
            Err(format!("server timed out; aborted and joined: {result:?}"))
        }
    };
    while worker.try_recv().is_ok() {}

    // All task owners have now been joined, including the failure paths.
    let observations = scenario.expect("receipt-fence scenario");
    terminal.expect("pipeline terminal admission");
    let report = joined.expect("pipeline worker join");
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let request = served.expect("loopback server join");
    assert!(request
        .head
        .starts_with("POST /v1/audio/stream/with-timestamps HTTP/1.1\r\n"));
    let headers = request.head.to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer synthetic\r\n"));
    assert!(headers.contains(&format!("speechify-version: {SPEECHIFY_API_VERSION}\r\n")));
    assert_eq!(
        request.body,
        json!({"input":TEXT, "voice_id":"geffen_32", "model":"simba-3.2",
        "output_format":format!("pcm_{source_rate}"), "language":"en-US",
        "options":{"loudness_normalization":false,"text_normalization":true}})
    );
    assert_eq!(observations.len(), 4);
    for observation in &observations {
        assert!(observation.text.is_empty(), "{observation:?}");
        assert_eq!(observation.committed, 0, "{}", observation.label);
    }
    assert_eq!(
        (observations[0].submissions, observations[0].receipts),
        (1, 0)
    );
    assert_eq!(
        (observations[1].submissions, observations[1].receipts),
        (1, 0)
    );
    assert_eq!(
        (observations[2].submissions, observations[2].receipts),
        (3, 2)
    );
    assert_eq!(
        (observations[3].submissions, observations[3].receipts),
        (3, 3)
    );
    assert_eq!(playback.active.load(Ordering::SeqCst), 0);
    assert_eq!(playback.stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        *playback.starts.lock().unwrap(),
        vec![(PcmFormat::new(output_rate, 1).unwrap(), vec![None])]
    );
    assert_eq!(
        *playback.receipts.lock().unwrap(),
        vec![
            PlaybackReceipt::Played,
            PlaybackReceipt::Played,
            if discard_last {
                PlaybackReceipt::Discarded
            } else {
                PlaybackReceipt::Played
            }
        ]
    );

    let before = probe.frames.lock().unwrap();
    let after = after_output.0.lock().unwrap();
    let words: Vec<_> = before
        .iter()
        .filter_map(|frame| match &frame.data {
            FrameData::TtsText(word) => Some((frame, word)),
            _ => None,
        })
        .collect();
    assert_eq!(
        words
            .iter()
            .map(|(_, word)| word.text.text.as_str())
            .collect::<Vec<_>>(),
        ["text-to-speech", "isn’t", "naïve."]
    );
    assert_eq!(
        words
            .iter()
            .map(|(_, word)| word.raw_text.as_deref().unwrap_or(""))
            .collect::<String>(),
        TEXT
    );
    let context_id = words[0].1.context_id.as_ref().unwrap();
    assert!(!context_id.is_empty());
    for (frame, word) in &words {
        assert_eq!(word.context_id.as_ref(), Some(context_id));
        assert!(word.will_be_spoken && frame.has_pipeline_pts());
        assert_eq!(frame.metadata["source_frame_id"], json!(input_id));
        assert_eq!(frame.metadata["alignment_evidence"], "provider_report");
        assert_ne!(frame.metadata.get("spoken"), Some(&json!(true)));
    }
    let rebased_words: Vec<_> = after
        .iter()
        .filter(|frame| matches!(frame.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(
        rebased_words
            .iter()
            .map(|frame| frame.id)
            .collect::<Vec<_>>(),
        words.iter().map(|(frame, _)| frame.id).collect::<Vec<_>>()
    );
    assert!(rebased_words.iter().all(|frame| !frame.has_pipeline_pts()));
    let baseline = words[0].0.pts.unwrap();
    assert!(baseline >= 9_000_000_000);
    assert_eq!(words[1].0.pts.unwrap() - baseline, 10_000_000);
    assert_eq!(words[2].0.pts.unwrap() - baseline, 20_000_000);
    let stopped: Vec<_> = before
        .iter()
        .filter(|frame| {
            matches!(&frame.data,
        FrameData::TtsStopped(stop) if stop.context_id.as_ref() == Some(context_id))
        })
        .collect();
    assert_eq!(stopped.len(), 1);
    assert_eq!(
        stopped[0].pts.unwrap() - baseline,
        30_000_001,
        "provider duration includes trailing silence after the final mark"
    );
    let source_pcm: Vec<_> = before
        .iter()
        .filter_map(|frame| match &frame.data {
            FrameData::TtsAudio(audio) => Some(audio),
            _ => None,
        })
        .collect();
    assert_eq!(source_pcm.len(), 2);
    assert_eq!(
        source_pcm
            .iter()
            .flat_map(|audio| audio.audio.iter().copied())
            .collect::<Vec<_>>(),
        pcm
    );
    let submitted = playback.submitted.lock().unwrap();
    let native_pcm: Vec<_> = submitted
        .iter()
        .map(|frame| match &frame.data {
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => audio,
            _ => panic!("native backend received a non-PCM frame"),
        })
        .collect();
    assert_eq!(native_pcm.len(), 3);
    for audio in &source_pcm {
        assert_eq!(audio.sample_rate, source_rate);
    }
    for audio in &native_pcm {
        assert_eq!(audio.sample_rate, output_rate);
    }
    for audio in source_pcm.iter().chain(native_pcm.iter()) {
        assert_eq!(audio.num_channels, 1);
        assert_eq!(audio.context_id.as_ref(), Some(context_id));
    }
    assert!(native_pcm
        .iter()
        .all(|audio| audio.num_frames() == (output_rate / 100) as usize));
    let source_frames: usize = source_pcm.iter().map(|audio| audio.num_frames()).sum();
    let output_frames: usize = native_pcm.iter().map(|audio| audio.num_frames()).sum();
    assert_eq!(source_frames, (source_rate * 30 / 1000) as usize);
    assert_eq!(output_frames, (output_rate * 30 / 1000) as usize);
    assert_eq!(
        source_frames as u64 * 1_000_000_000 / u64::from(source_rate),
        30_000_000
    );
    assert_eq!(
        output_frames as u64 * 1_000_000_000 / u64::from(output_rate),
        30_000_000
    );
    assert_eq!(
        native_pcm
            .iter()
            .flat_map(|audio| audio.audio.iter().copied())
            .collect::<Vec<_>>(),
        resample_pcm16(&pcm, source_rate, output_rate, 1).unwrap(),
        "native output must transform samples, drain the resampler tail, and preserve duration"
    );
    for (id, key) in [
        (start_id, "test.original-start"),
        (end_id, "test.original-end"),
    ] {
        for frames in [&*before, &*after] {
            let found: Vec<_> = frames.iter().filter(|frame| frame.id == id).collect();
            assert_eq!(
                found.len(),
                1,
                "original source identity must cross exactly once"
            );
            assert_eq!(found[0].metadata[key], true);
            assert_ne!(found[0].metadata.get("failed"), Some(&json!(true)));
        }
    }
    assert_eq!(
        history.status().committed_contexts,
        u64::from(!discard_last)
    );
    assert_eq!(history.status().withheld_contexts, u64::from(discard_last));
    assert_eq!(
        text(&shared).unwrap(),
        if discard_last {
            vec![]
        } else {
            vec![TEXT.to_owned()]
        }
    );
}

#[tokio::test]
async fn speechify_whole_words_enter_assistant_history_once_after_all_played_receipts() {
    check_receipt_fence(false, RATE).await;
}

#[tokio::test]
async fn speechify_done_and_partial_playback_cannot_commit_a_discarded_context() {
    check_receipt_fence(true, RATE).await;
}

#[tokio::test]
async fn speechify_24k_fallback_is_resampled_to_32k_before_playback_history_receipts() {
    // The real provider request uses pcm_24000. Native output must receive
    // three 320-frame writes, totaling 960 frames from 720 source frames.
    check_receipt_fence(false, 32000).await;
}
