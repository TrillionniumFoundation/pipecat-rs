//! Conversation-only input admission pause over real localhost WS + production Pipeline.
//! No live microphone/provider claim. Pause does not retract already admitted work.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;

// Keep directly affected old contracts in the same test artifact/dependency graph.
#[path = "live.rs"]
mod live_regressions;
#[path = "stt_mute.rs"]
mod stt_mute_regressions;

const WAIT: Duration = Duration::from_secs(5);
const OUTPUT_PCM: &[u8] = &[91, 0, 92, 0];

enum Step {
    Frame(Box<Frame>, Direction),
    Pause(bool),
    Mute(bool, Direction),
    Fence(&'static str),
    Finalize(usize),
}
fn down(frame: Frame) -> Step {
    Step::Frame(Box::new(frame), Direction::Downstream)
}
fn audio(user: bool, bytes: Vec<u8>) -> Frame {
    wrapped(user, AudioRawFrame::new(bytes, 16_000, 1).unwrap())
}
fn wrapped(user: bool, audio: AudioRawFrame) -> Frame {
    Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "private-speaker".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    })
}
fn invalid(user: bool) -> Frame {
    wrapped(
        user,
        AudioRawFrame {
            audio: vec![1, 2, 3],
            sample_rate: 0,
            num_channels: 0,
            context_id: Some("x".repeat(1025)),
        },
    )
}
fn config(manual: bool) -> GeminiConfig {
    let mut config = GeminiConfig::new("audio-pause-key", "audio-pause-model");
    config.session.activity_detection.disabled = manual;
    config
}
fn start() -> Step {
    down(Frame::new(FrameData::UserStartedSpeaking))
}
fn stop() -> Step {
    down(Frame::new(FrameData::UserStoppedSpeaking))
}
#[derive(Debug)]
struct Trace {
    setup: Value,
    wire: Vec<Value>,
    closed: bool,
}
#[derive(Debug)]
struct Observed {
    seen: Vec<DirectedFrame>,
    report: WorkerReport,
}

async fn next(worker: &mut PipelineWorker, seen: &mut Vec<DirectedFrame>) -> Result<(), String> {
    let frame = worker
        .recv()
        .await
        .ok_or("pipeline closed before witness")?;
    if matches!(
        frame.frame.data,
        FrameData::Error(_) | FrameData::ErrorDetailed(_)
    ) {
        return Err(format!("unexpected output error: {:?}", frame.frame.data));
    }
    seen.push(frame);
    Ok(())
}
async fn forwarded(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    mut frame: Frame,
    direction: Direction,
) -> Result<(), String> {
    frame.name = "audio-pause-original-envelope".into();
    frame.set_pipeline_pts(1234);
    frame.transport_source = Some("source".into());
    frame.transport_destination = Some("destination".into());
    frame.broadcast_sibling_id = Some(4567);
    frame.interruptible = true;
    frame
        .metadata
        .insert("witness".into(), json!({"original":true}));
    let tag = LocalFrameTag::new();
    tag.mark(&mut frame).map_err(|e| e.to_string())?;
    let expected = frame.clone();
    worker
        .sender()
        .send(frame, direction)
        .await
        .map_err(|e| e.to_string())?;
    loop {
        if let Some(actual) = seen
            .iter()
            .find(|f| f.frame.id == expected.id && f.direction == direction)
        {
            if actual.frame != expected || !tag.contains(&actual.frame) {
                return Err("forwarded frame/envelope/local identity changed".into());
            }
            return Ok(());
        }
        next(worker, seen).await?;
    }
}
fn response_seen(seen: &[DirectedFrame], marker: &str, transcription: bool) -> bool {
    if transcription {
        return seen.iter().any(|f| {
            matches!(&f.frame.data,
            FrameData::Transcription(t) if t.text == marker && t.finalized)
        });
    }
    let mut matched = false;
    for frame in seen {
        match &frame.frame.data {
            FrameData::LlmText(text) if text.text == marker => matched = true,
            FrameData::LlmResponseEnd if matched => return true,
            _ => {}
        }
    }
    false
}
async fn wait_response(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    marker: &str,
    transcription: bool,
) -> Result<(), String> {
    // The response may already have been consumed while observing passthrough.
    while !response_seen(seen, marker, transcription) {
        next(worker, seen).await?;
    }
    Ok(())
}
async fn drain_join(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
) -> Result<WorkerReport, String> {
    while let Some(frame) = worker.recv().await {
        seen.push(frame);
    }
    worker.join().await.map_err(|e| e.to_string())
}

async fn execute(
    mut config: GeminiConfig,
    initial_pause: bool,
    preroll_ms: Option<u32>,
    sessions: Vec<Vec<Step>>,
) -> (Vec<Trace>, Vec<Observed>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let transcription = config.mode == SessionMode::Transcription;
    let session_count = sessions.len();
    let mut peer = tokio::spawn(async move {
        let mut traces = vec![];
        for _ in 0..session_count {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
                .await
                .map_err(|_| "upgrade deadline")?
                .map_err(|e| e.to_string())?;
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "setup deadline")?
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            let setup: Value = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let mut trace = Trace {
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
            .map_err(|_| "ready deadline")?
            .map_err(|e| e.to_string())?;
            let mut finalizations = 0;
            loop {
                match timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "peer read deadline")?
                {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        let marker = value["realtimeInput"]["text"].as_str().map(str::to_owned);
                        let finalizes = value["realtimeInput"].get("audioStreamEnd").is_some();
                        trace.wire.push(value);
                        let reply = if finalizes {
                            finalizations += 1;
                            Some(
                                json!({"serverContent":{"inputTranscription":{"text":format!("stt-final-{finalizations}"),"languageCode":"en-US"},"turnComplete":true}}),
                            )
                        } else {
                            marker.map(|marker| if transcription {
                                json!({"serverContent":{"inputTranscription":{"text":marker,"languageCode":"en-US"},"turnComplete":true}})
                            } else {
                                json!({"serverContent":{
                                    "modelTurn":{"parts":[{"text":marker},{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":STANDARD.encode(OUTPUT_PCM)}}]},
                                    "inputTranscription":{"text":format!("heard-{marker}"),"languageCode":"en-US"},
                                    "outputTranscription":{"text":format!("spoken-{marker}")},
                                    "turnComplete":true}})
                            })
                        };
                        if let Some(reply) = reply {
                            timeout(WAIT, ws.send(Message::Text(reply.to_string().into())))
                                .await
                                .map_err(|_| "reply deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        timeout(WAIT, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        trace.closed = true;
                        timeout(WAIT, ws.flush())
                            .await
                            .map_err(|_| "close flush deadline")?
                            .map_err(|e| e.to_string())?;
                        break;
                    }
                    other => return Err(format!("expected actual WS Close: {other:?}")),
                }
            }
            traces.push(trace);
        }
        Ok::<_, String>(traces)
    });
    let mut processor = GeminiLiveProcessor::new(GeminiClient::new(config).unwrap())
        .with_audio_input_paused(initial_pause);
    if let Some(milliseconds) = preroll_ms {
        processor = processor.with_preroll_ms(milliseconds).unwrap();
    }
    // This is the same production instance moved into Pipeline, not a test-only control.
    let processor = Arc::new(processor);
    let mut observations = vec![];
    let mut failures = vec![];
    for steps in sessions {
        // Let core enforce startup deadlines and settle its setup task/supervisor.
        // Dropping an outer timeout future would lose the returned owner join witness.
        let created = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                startup_timeout: WAIT,
                shutdown_timeout: Duration::from_secs(2),
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
                failures.push(format!("pipeline setup failed: {error}"));
                break;
            }
        };
        let mut seen = vec![];
        let observed = timeout(WAIT, async {
            while !seen.iter().any(|f: &DirectedFrame| f.frame.data.is_start()) {
                next(&mut worker, &mut seen).await?;
            }
            for step in steps {
                match step {
                    Step::Pause(paused) => processor.set_audio_input_paused(paused),
                    Step::Frame(frame, direction) => {
                        forwarded(&mut worker, &mut seen, *frame, direction).await?
                    }
                    Step::Mute(muted, direction) => {
                        let control = Frame::new(FrameData::SttMute(muted));
                        let id = control.id;
                        worker
                            .sender()
                            .send(control, direction)
                            .await
                            .map_err(|e| e.to_string())?;
                        // SttMute is System; Text is Normal. FrameQueue::recv_normal
                        // waits for both queued and in-progress System work to finish.
                        // Thus this same-direction witness cannot overtake consumed mute.
                        forwarded(
                            &mut worker,
                            &mut seen,
                            Frame::text("mute-local-fence"),
                            direction,
                        )
                        .await?;
                        if seen.iter().any(|f| f.frame.id == id) {
                            return Err("dedicated STT leaked consumed mute control".into());
                        }
                    }
                    Step::Fence(marker) => {
                        forwarded(
                            &mut worker,
                            &mut seen,
                            Frame::new(FrameData::InputText(TextFrame::new(marker))),
                            Direction::Downstream,
                        )
                        .await?;
                        // Ordered actor command + real peer response, not a sleep-based absence check.
                        wait_response(&mut worker, &mut seen, marker, transcription).await?;
                    }
                    Step::Finalize(sequence) => {
                        forwarded(
                            &mut worker,
                            &mut seen,
                            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                                duration_secs: 0.2,
                                timestamp: 0.0,
                            })),
                            Direction::Downstream,
                        )
                        .await?;
                        wait_response(
                            &mut worker,
                            &mut seen,
                            &format!("stt-final-{sequence}"),
                            true,
                        )
                        .await?;
                    }
                }
            }
            if !worker
                .processor_health(0)
                .ok_or("missing health")?
                .is_usable()
                || !worker.unusable_processors().is_empty()
            {
                return Err("pause changed processor usability".into());
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "observation deadline".to_owned())
        .and_then(|r| r);
        let normal = observed.is_ok();
        if let Err(error) = observed {
            failures.push(error);
        }
        let admission = timeout(WAIT, async {
            if normal {
                worker.end().await
            } else {
                worker.cancel("audio-pause failed observation").await
            }
        })
        .await;
        if !matches!(admission, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admission:?}"));
            let _ = timeout(WAIT, worker.cancel("audio-pause terminal cleanup")).await;
        }
        let joined = match timeout(WAIT, drain_join(&mut worker, &mut seen)).await {
            Ok(result) => result,
            Err(error) => {
                failures.push(format!("owner join deadline: {error}"));
                let _ = timeout(WAIT, worker.cancel("audio-pause join cleanup")).await;
                // Cancel stops owned receive work; always settle its actual owner before peer assertions.
                drain_join(&mut worker, &mut seen).await
            }
        };
        match joined {
            Ok(report) => observations.push(Observed { seen, report }),
            Err(error) => failures.push(error),
        }
        if !failures.is_empty() {
            break;
        }
    }
    let traces = match timeout(WAIT, &mut peer).await {
        Ok(result) => result.map_err(|e| e.to_string()).and_then(|r| r),
        Err(error) => {
            peer.abort();
            let _ = peer.await;
            Err(format!("peer deadline; aborted and joined: {error}"))
        }
    };
    assert!(failures.is_empty(), "{failures:?}; peer={traces:?}");
    let traces = traces.unwrap();
    assert_eq!(traces.len(), observations.len());
    for (trace, observed) in traces.iter().zip(&observations) {
        assert!(trace.closed, "normal owner End must produce peer WS Close");
        assert_eq!(trace.setup["setup"]["model"], "models/audio-pause-model");
        assert!(trace.wire.iter().all(|v| v.get("setup").is_none()));
        assert_eq!(observed.report.state, WorkerState::Ended);
        assert_eq!(observed.report.total_errors, 0);
        assert!(observed.report.errors.is_empty());
        if transcription {
            assert!(observed
                .seen
                .iter()
                .all(|f| !matches!(f.frame.data, FrameData::SttMute(_))));
        }
    }
    (traces, observations)
}
fn pcm(trace: &Trace) -> Vec<Vec<u8>> {
    trace
        .wire
        .iter()
        .filter_map(|v| {
            v["realtimeInput"]["audio"]["data"].as_str().map(|data| {
                assert_eq!(
                    v["realtimeInput"]["audio"]["mimeType"],
                    "audio/pcm;rate=16000"
                );
                STANDARD.decode(data).unwrap()
            })
        })
        .collect()
}
fn audio_wire(bytes: &[u8]) -> Value {
    json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":STANDARD.encode(bytes)}}})
}
fn text_wire(text: &str) -> Value {
    json!({"realtimeInput":{"text":text}})
}

#[tokio::test]
async fn paused_audio_families_skip_validation_then_resume_once_without_replay() {
    let (traces, _) = execute(
        config(false),
        true,
        None,
        vec![vec![
            down(audio(false, vec![10, 0])),
            down(audio(true, vec![20, 0])),
            down(invalid(false)),
            down(invalid(true)),
            Step::Fence("initially-paused"),
            Step::Pause(false),
            down(audio(false, vec![1, 0])),
            down(audio(true, vec![2, 0])),
            Step::Pause(true),
            down(audio(false, vec![30, 0])),
            down(audio(true, vec![40, 0])),
            Step::Fence("paused-again"),
            Step::Pause(false),
            down(audio(false, vec![3, 0])),
            down(audio(true, vec![4, 0])),
            Step::Fence("resumed"),
        ]],
    )
    .await;
    assert_eq!(
        traces[0].wire,
        vec![
            text_wire("initially-paused"),
            audio_wire(&[1, 0]),
            audio_wire(&[2, 0]),
            text_wire("paused-again"),
            audio_wire(&[3, 0]),
            audio_wire(&[4, 0]),
            text_wire("resumed")
        ]
    );
}

#[tokio::test]
async fn pause_retains_old_manual_preroll_but_does_not_buffer_new_audio() {
    let (traces, _) = execute(
        config(true),
        false,
        Some(1),
        vec![vec![
            down(audio(false, vec![7; 40])),
            Step::Pause(true),
            down(audio(true, vec![8; 40])),
            start(),
            down(audio(false, vec![9, 0])),
            stop(),
            down(audio(true, vec![12, 0])),
            start(),
            stop(),
            Step::Pause(false),
            down(audio(true, vec![10, 0])),
            start(),
            down(audio(false, vec![11, 0])),
            stop(),
            Step::Fence("manual-complete"),
        ]],
    )
    .await;
    let start = json!({"realtimeInput":{"activityStart":{}}});
    let stop = json!({"realtimeInput":{"activityEnd":{}}});
    assert_eq!(
        traces[0].wire,
        vec![
            start.clone(),
            audio_wire(&[7; 32]),
            stop.clone(),
            start.clone(),
            stop.clone(),
            start,
            audio_wire(&[10, 0]),
            audio_wire(&[11, 0]),
            stop,
            text_wire("manual-complete")
        ]
    );
}

#[tokio::test]
async fn paused_conversation_keeps_text_provider_outputs_and_noninput_directions_live() {
    let (traces, observed) = execute(
        config(false),
        true,
        None,
        vec![vec![
            Step::Frame(Box::new(invalid(false)), Direction::Upstream),
            Step::Frame(Box::new(invalid(true)), Direction::Upstream),
            down(Frame::new(FrameData::OutputAudio(
                AudioRawFrame::new(vec![5, 0], 16_000, 1).unwrap(),
            ))),
            down(Frame::new(FrameData::SttMute(true))),
            Step::Fence("while-paused"),
            down(audio(true, vec![6, 0])),
            Step::Fence("still-paused"),
        ]],
    )
    .await;
    assert_eq!(
        traces[0].wire,
        vec![text_wire("while-paused"), text_wire("still-paused")]
    );
    let seen = &observed[0].seen;
    for marker in ["while-paused", "still-paused"] {
        assert!(response_seen(seen, marker, false));
        assert!(seen.iter().any(|f| matches!(&f.frame.data, FrameData::InterimTranscription(t) if t.text == format!("heard-{marker}"))));
        assert!(seen.iter().any(|f| matches!(&f.frame.data, FrameData::TtsText(t) if t.text.text == format!("spoken-{marker}"))));
    }
    let outputs: Vec<_> = seen.iter().filter(|f| matches!(&f.frame.data, FrameData::TtsAudio(a) if a.audio == OUTPUT_PCM && a.sample_rate == 24_000)).collect();
    assert_eq!(outputs.len(), 2);
    assert!(outputs.iter().all(|f| f.direction == Direction::Downstream));
}

#[tokio::test]
async fn dedicated_transcription_ignores_audio_pause_and_keeps_stt_mute_finalization() {
    let config = GeminiConfig::transcription("audio-pause-key", "audio-pause-model");
    let (traces, observed) = execute(
        config,
        true,
        None,
        vec![vec![
            down(audio(false, vec![1, 0])),
            Step::Mute(true, Direction::Downstream),
            down(audio(true, vec![2, 0])),
            Step::Pause(false),
            down(audio(false, vec![3, 0])),
            Step::Finalize(1),
            Step::Mute(false, Direction::Upstream),
            Step::Pause(true),
            down(audio(true, vec![4, 0])),
            Step::Finalize(2),
        ]],
    )
    .await;
    let finalize = json!({"realtimeInput":{"audioStreamEnd":true}});
    assert_eq!(
        traces[0].wire,
        vec![
            audio_wire(&[1, 0]),
            finalize.clone(),
            audio_wire(&[4, 0]),
            finalize
        ]
    );
    for sequence in 1..=2 {
        assert!(response_seen(
            &observed[0].seen,
            &format!("stt-final-{sequence}"),
            true
        ));
    }
}

#[tokio::test]
async fn sequential_setup_restores_builder_pause_instead_of_python_dynamic_state() {
    // Pinned Python llm.py @af9f9d writes pause only in construction/setter (777/907).
    // Native completed reuse follows video pause: each setup restores builder state.
    for initial in [false, true] {
        let (traces, _) = execute(
            config(false),
            initial,
            None,
            vec![
                vec![
                    Step::Pause(!initial),
                    down(audio(false, vec![1, 0])),
                    Step::Fence("first-session"),
                ],
                vec![down(audio(true, vec![2, 0])), Step::Fence("second-session")],
            ],
        )
        .await;
        assert_eq!(
            pcm(&traces[0]),
            if initial { vec![vec![1, 0]] } else { vec![] }
        );
        assert_eq!(
            pcm(&traces[1]),
            if initial { vec![] } else { vec![vec![2, 0]] }
        );
        assert_eq!(traces[0].wire.len(), if initial { 2 } else { 1 });
        assert_eq!(traces[1].wire.len(), if initial { 1 } else { 2 });
        assert_eq!(
            traces[0].setup, traces[1].setup,
            "local pause must not alter provider setup"
        );
    }
}
