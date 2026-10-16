//! Dedicated STT labels are mapping-time observations, not audio/result correlation.
//! Real localhost WebSocket + production Pipeline; no hosted-provider claim.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::tungstenite::Message;

// Preserve the affected old contracts in this artifact, including their fixtures.
#[path = "audio_pause.rs"]
mod audio_pause_regressions;
#[path = "input_audio_family.rs"]
mod input_audio_family_regressions;

const WAIT: Duration = Duration::from_secs(5);

enum Step {
    Frame(Box<Frame>, Direction),
    Mute(bool, Direction),
    Rejected(Box<Frame>, ErrorCategory, &'static str),
    Reply {
        text: String,
        finalized: bool,
        label: &'static str,
        after_audio: usize,
    },
}
struct Session {
    steps: Vec<Step>,
    end_label: Option<&'static str>,
}
struct Reply {
    text: String,
    finalized: bool,
    after_audio: usize,
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
fn audio(user: Option<&str>, bytes: Vec<u8>) -> Frame {
    wrapped(user, AudioRawFrame::new(bytes, 16_000, 1).unwrap())
}
fn wrapped(user: Option<&str>, audio: AudioRawFrame) -> Frame {
    Frame::new(match user {
        Some(user_id) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user_id.into(),
        }),
        None => FrameData::InputAudio(audio),
    })
}
fn down(frame: Frame) -> Step {
    Step::Frame(Box::new(frame), Direction::Downstream)
}
fn reply(
    text: impl Into<String>,
    finalized: bool,
    label: &'static str,
    after_audio: usize,
) -> Step {
    Step::Reply {
        text: text.into(),
        finalized,
        label,
        after_audio,
    }
}
fn audio_wire(bytes: &[u8]) -> Value {
    json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":STANDARD.encode(bytes)}}})
}
fn response(reply: &Reply) -> Value {
    if reply.finalized {
        json!({"serverContent":{"inputTranscription":{"text":reply.text,"languageCode":"en-US"},"turnComplete":true}})
    } else {
        json!({"serverContent":{"interimInputTranscription":{"text":reply.text,"languageCode":"en-US"}}})
    }
}
async fn next(worker: &mut PipelineWorker, seen: &mut Vec<DirectedFrame>) -> Result<(), String> {
    seen.push(
        worker
            .recv()
            .await
            .ok_or("pipeline closed before witness")?,
    );
    Ok(())
}
async fn forwarded(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    mut frame: Frame,
    direction: Direction,
) -> Result<(), String> {
    frame.name = "stt-speaker-original-envelope".into();
    frame.set_pipeline_pts(1234);
    frame.transport_source = Some("source".into());
    frame.transport_destination = Some("destination".into());
    frame.broadcast_sibling_id = Some(4567);
    frame.interruptible = true;
    frame
        .metadata
        .insert("context_id".into(), json!("not-a-label"));
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
fn transcript(frame: &DirectedFrame) -> Option<&TranscriptionFrame> {
    match &frame.frame.data {
        FrameData::Transcription(t) | FrameData::InterimTranscription(t) => Some(t),
        _ => None,
    }
}
async fn wait_reply(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    text: &str,
    finalized: bool,
    label: &str,
) -> Result<(), String> {
    loop {
        // Other witnesses can already have consumed this real peer response.
        if let Some((frame, t)) = seen
            .iter()
            .filter_map(|f| transcript(f).map(|t| (f, t)))
            .find(|(_, t)| t.text == text && t.finalized == finalized)
        {
            if t.user_id != label
                || t.language.as_deref() != Some("en-US")
                || t.timestamp.is_empty()
                || frame.direction != Direction::Downstream
                || matches!(frame.frame.data, FrameData::Transcription(_)) != finalized
                || frame.frame.metadata.contains_key("source_frame_id")
                || frame.frame.metadata.contains_key("context_id")
            {
                return Err(format!(
                    "wrong transcript for {text:?}: {frame:?}; expected label {label:?}"
                ));
            }
            return Ok(());
        }
        next(worker, seen).await?;
    }
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
async fn execute(fixed: Option<&str>, sessions: Vec<Session>) -> (Vec<Trace>, Vec<Observed>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::transcription("speaker-key", "speaker-model");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    config.limits.max_audio_bytes = 32;
    config.limits.finish_timeout = Duration::from_secs(2);
    let count = sessions.len();
    let (commands, mut replies) = mpsc::channel::<Reply>(1);
    let mut peer = tokio::spawn(async move {
        let mut traces = vec![];
        for session in 0..count {
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
            let mut audio_count = 0;
            let mut pending: Option<Reply> = None;
            loop {
                if pending
                    .as_ref()
                    .is_some_and(|r| audio_count >= r.after_audio)
                {
                    let r = pending.take().ok_or("lost pending reply")?;
                    timeout(
                        WAIT,
                        ws.send(Message::Text(response(&r).to_string().into())),
                    )
                    .await
                    .map_err(|_| "reply deadline")?
                    .map_err(|e| e.to_string())?;
                }
                enum Wake {
                    Wire(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                    Reply(Option<Reply>),
                }
                let wake = timeout(WAIT, async {
                    tokio::select! {
                        message = ws.next() => Wake::Wire(message),
                        reply = replies.recv(), if pending.is_none() => Wake::Reply(reply),
                    }
                })
                .await
                .map_err(|_| "peer observation deadline")?;
                match wake {
                    Wake::Reply(Some(r)) => pending = Some(r),
                    Wake::Reply(None) => return Err("reply control closed before WS Close".into()),
                    Wake::Wire(Some(Ok(Message::Text(text)))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        if value["realtimeInput"].get("audio").is_some() {
                            audio_count += 1;
                        }
                        let end = value["realtimeInput"].get("audioStreamEnd").is_some();
                        trace.wire.push(value);
                        if end {
                            // Only the real End/finish command unlocks this final result.
                            let r = Reply {
                                text: format!("end-drain-{session}"),
                                finalized: true,
                                after_audio: 0,
                            };
                            timeout(
                                WAIT,
                                ws.send(Message::Text(response(&r).to_string().into())),
                            )
                            .await
                            .map_err(|_| "end reply deadline")?
                            .map_err(|e| e.to_string())?;
                        }
                    }
                    Wake::Wire(Some(Ok(Message::Ping(bytes)))) => {
                        timeout(WAIT, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Wake::Wire(Some(Ok(Message::Pong(_)))) => {}
                    Wake::Wire(Some(Ok(Message::Close(_)))) => {
                        if pending.is_some() {
                            return Err("WS closed with an unfulfilled reply".into());
                        }
                        trace.closed = true;
                        timeout(WAIT, ws.flush())
                            .await
                            .map_err(|_| "close flush deadline")?
                            .map_err(|e| e.to_string())?;
                        break;
                    }
                    Wake::Wire(other) => {
                        return Err(format!("expected actual WS Close: {other:?}"));
                    }
                }
            }
            traces.push(trace);
        }
        Ok::<_, String>(traces)
    });
    let mut processor = GeminiLiveProcessor::new(GeminiClient::new(config).unwrap());
    if let Some(user_id) = fixed {
        processor = processor.with_user_id(user_id);
    }
    let processor = Arc::new(processor);
    let mut observations = vec![];
    let mut failures = vec![];
    for (session_index, session) in sessions.into_iter().enumerate() {
        // Core owns the startup deadline and joins its setup task. Never drop an
        // outer timeout future that could lose the newly returned Pipeline owner.
        let created = Pipeline::new(vec![processor.clone()])
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
                failures.push(format!("pipeline setup: {error}"));
                break;
            }
        };
        let expected_errors = session
            .steps
            .iter()
            .filter(|s| matches!(s, Step::Rejected(..)))
            .count();
        let mut expected_replies: Vec<_> = session
            .steps
            .iter()
            .filter_map(|s| match s {
                Step::Reply {
                    text,
                    finalized,
                    label,
                    ..
                } => Some((text.clone(), *finalized, *label)),
                _ => None,
            })
            .collect();
        if let Some(label) = session.end_label {
            expected_replies.push((format!("end-drain-{session_index}"), true, label));
        }
        let mut seen = vec![];
        let mut rejected_ids = vec![];
        let observed = timeout(WAIT, async {
            while !seen.iter().any(|f: &DirectedFrame| f.frame.data.is_start()) {
                next(&mut worker, &mut seen).await?;
            }
            for step in session.steps {
                match step {
                    Step::Frame(frame, direction) => {
                        forwarded(&mut worker, &mut seen, *frame, direction).await?
                    }
                    Step::Mute(muted, direction) => {
                        let frame = Frame::new(FrameData::SttMute(muted));
                        let id = frame.id;
                        worker
                            .sender()
                            .send(frame, direction)
                            .await
                            .map_err(|e| e.to_string())?;
                        // Normal same-lane work waits for queued/in-flight System mute.
                        forwarded(
                            &mut worker,
                            &mut seen,
                            Frame::text("mute-local-fence"),
                            direction,
                        )
                        .await?;
                        if seen.iter().any(|f| f.frame.id == id) {
                            return Err("STT mute leaked".into());
                        }
                    }
                    Step::Rejected(frame, category, message) => {
                        rejected_ids.push(frame.id);
                        let errors_before = seen
                            .iter()
                            .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                            .count();
                        worker
                            .sender()
                            .send(*frame, Direction::Downstream)
                            .await
                            .map_err(|e| e.to_string())?;
                        loop {
                            if let Some(actual) = seen
                                .iter()
                                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                                .nth(errors_before)
                            {
                                if let FrameData::Error(e) = &actual.frame.data {
                                    if actual.direction != Direction::Upstream
                                        || e.fatal
                                        || e.category != category
                                        || !e.error.contains(message)
                                    {
                                        return Err(format!("unexpected rejection: {actual:?}"));
                                    }
                                }
                                break;
                            }
                            next(&mut worker, &mut seen).await?;
                        }
                    }
                    Step::Reply {
                        text,
                        finalized,
                        label,
                        after_audio,
                    } => {
                        // This command is issued only after the preceding exact audio
                        // outlet/error witness. Wire receipt alone is not the label fence.
                        commands
                            .send(Reply {
                                text: text.clone(),
                                finalized,
                                after_audio,
                            })
                            .await
                            .map_err(|e| e.to_string())?;
                        wait_reply(&mut worker, &mut seen, &text, finalized, label).await?;
                    }
                }
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "pipeline observation deadline".to_owned())
        .and_then(|r| r);
        let normal = observed.is_ok();
        if let Err(error) = observed {
            failures.push(error);
        }
        let admitted = timeout(WAIT, async {
            if normal {
                worker.end().await
            } else {
                worker.cancel("speaker-label observation failed").await
            }
        })
        .await;
        if !matches!(admitted, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admitted:?}"));
            let _ = timeout(WAIT, worker.cancel("speaker-label terminal cleanup")).await;
        }
        let joined = match timeout(WAIT, drain_join(&mut worker, &mut seen)).await {
            Ok(report) => report,
            Err(error) => {
                failures.push(format!("owner join deadline: {error}"));
                let _ = timeout(WAIT, worker.cancel("speaker-label join cleanup")).await;
                // Cancellation settles owned receive work; still join the actual owner.
                drain_join(&mut worker, &mut seen).await
            }
        };
        match joined {
            Ok(report) => {
                if normal {
                    if report.state != WorkerState::Ended
                        || report.total_errors != expected_errors as u64
                        || report.errors.len() != expected_errors
                        || report.evicted_errors != 0
                        || report.errors.iter().any(|e| e.fatal)
                        || seen.iter().any(|f| rejected_ids.contains(&f.frame.id))
                        || seen
                            .iter()
                            .any(|f| matches!(f.frame.data, FrameData::SttMute(_)))
                    {
                        failures.push(format!("unexpected final owner report: {report:?}"));
                    }
                    let actual: Vec<_> = seen
                        .iter()
                        .filter_map(transcript)
                        .map(|t| (t.text.clone(), t.finalized, t.user_id.as_str()))
                        .collect();
                    if actual != expected_replies {
                        failures.push(format!(
                            "transcript sequence: {actual:?}; expected {expected_replies:?}"
                        ));
                    }
                }
                observations.push(Observed { seen, report });
            }
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
    drop(commands);
    assert!(failures.is_empty(), "{failures:?}; peer={traces:?}");
    let traces = traces.unwrap();
    assert_eq!(traces.len(), observations.len());
    for trace in &traces {
        assert!(trace.closed, "normal End must produce actual peer WS Close");
        assert_eq!(trace.setup["setup"]["model"], "models/speaker-model");
        assert!(trace.wire.iter().all(|value| value.get("setup").is_none()));
    }
    (traces, observations)
}

#[tokio::test]
async fn default_labels_follow_latest_audio_at_each_late_interim_and_final() {
    let (traces, _) = execute(
        None,
        vec![Session {
            steps: vec![
                reply("before-audio", true, "", 0),
                down(audio(Some("alice"), vec![1, 0])),
                reply("old-utterance-interim", false, "alice", 1),
                down(audio(Some("bob"), vec![2, 0])),
                reply("old-utterance-final", true, "bob", 2),
                down(audio(None, vec![3, 0])),
                reply("plain-nonempty", true, "", 3),
                down(audio(Some("empty-pcm-speaker"), vec![])),
                reply("empty-user-pcm", false, "empty-pcm-speaker", 3),
                down(audio(Some(""), vec![4, 0])),
                reply("empty-user-id", true, "", 4),
                down(audio(Some("carol"), vec![5, 0])),
                down(audio(None, vec![])),
                reply("empty-plain-pcm", true, "", 5),
            ],
            end_label: None,
        }],
    )
    .await;
    assert_eq!(
        traces[0].wire,
        vec![
            audio_wire(&[1, 0]),
            audio_wire(&[2, 0]),
            audio_wire(&[3, 0]),
            audio_wire(&[4, 0]),
            audio_wire(&[5, 0])
        ]
    );
}

#[tokio::test]
async fn muted_upstream_and_invalid_audio_keep_labels_but_enqueue_failure_keeps_new_label() {
    let mut steps = vec![
        down(audio(Some("accepted"), vec![1, 0])),
        reply("before-mute", false, "accepted", 1),
        Step::Mute(true, Direction::Downstream),
    ];
    for user in [None, Some("muted-speaker")] {
        steps.push(down(audio(user, vec![2, 0])));
        steps.push(down(audio(user, vec![])));
        steps.push(down(wrapped(
            user,
            AudioRawFrame {
                audio: vec![1, 2, 3],
                sample_rate: 0,
                num_channels: 0,
                context_id: None,
            },
        )));
    }
    steps.push(reply("late-while-muted", true, "accepted", 1));
    steps.push(Step::Mute(false, Direction::Upstream));
    for user in [None, Some("upstream-speaker")] {
        steps.push(Step::Frame(
            Box::new(audio(user, vec![3, 0])),
            Direction::Upstream,
        ));
        steps.push(Step::Frame(
            Box::new(audio(user, vec![])),
            Direction::Upstream,
        ));
    }
    steps.push(reply("after-upstream", true, "accepted", 1));
    for user in [None, Some("invalid-speaker")] {
        for case in 0..8 {
            let mut pcm = AudioRawFrame::new(vec![4, 0, 5, 0], 16_000, 1).unwrap();
            let (category, message) = match case {
                0 => {
                    pcm.sample_rate = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                1 => {
                    pcm.num_channels = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                2 => {
                    pcm.audio.pop();
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                3 => {
                    pcm.num_channels = 2;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
                4 => {
                    pcm.sample_rate = 8_000;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
                5 => {
                    pcm.audio = vec![0; 34];
                    (ErrorCategory::Application, "configured limit")
                }
                6 => {
                    pcm.audio.clear();
                    pcm.sample_rate = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                _ => {
                    pcm.audio.clear();
                    pcm.sample_rate = 8_000;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
            };
            steps.push(Step::Rejected(
                Box::new(wrapped(user, pcm)),
                category,
                message,
            ));
            steps.push(reply(
                format!("invalid-{user:?}-{case}"),
                true,
                "accepted",
                1,
            ));
        }
    }
    for (user, label, marker) in [
        (
            Some("enqueue-rejected"),
            "enqueue-rejected",
            "rejected-user",
        ),
        (None, "", "rejected-plain"),
    ] {
        let mut pcm = AudioRawFrame::new(vec![6, 0], 16_000, 1).unwrap();
        // Valid PCM passes the processor boundary; the sender rejects this origin
        // inside enqueue. That failure must not roll back the new observed label.
        pcm.context_id = Some("x".repeat(1025));
        steps.push(Step::Rejected(
            Box::new(wrapped(user, pcm)),
            ErrorCategory::Application,
            "configured limit",
        ));
        steps.push(reply(marker, true, label, 1));
    }
    let (traces, observed) = execute(
        None,
        vec![Session {
            steps,
            end_label: None,
        }],
    )
    .await;
    assert_eq!(traces[0].wire, vec![audio_wire(&[1, 0])]);
    assert_eq!(observed[0].report.errors.len(), 18);
}

#[tokio::test]
async fn explicit_fixed_label_including_empty_wins_over_both_audio_families() {
    for fixed in ["configured-speaker", ""] {
        let (traces, _) = execute(
            Some(fixed),
            vec![Session {
                steps: vec![
                    reply("fixed-before-audio", true, fixed, 0),
                    down(audio(Some("alice"), vec![1, 0])),
                    reply("fixed-user", false, fixed, 1),
                    down(audio(None, vec![2, 0])),
                    reply("fixed-plain", true, fixed, 2),
                    down(audio(Some("empty-pcm"), vec![])),
                    down(audio(None, vec![])),
                    reply("fixed-empty", true, fixed, 2),
                ],
                end_label: None,
            }],
        )
        .await;
        assert_eq!(
            traces[0].wire,
            vec![audio_wire(&[1, 0]), audio_wire(&[2, 0])]
        );
    }
}

#[tokio::test]
async fn reused_processor_setup_resets_dynamic_label_and_preserves_explicit_config() {
    for fixed in [None, Some("configured-speaker"), Some("")] {
        let first = fixed.unwrap_or("first-session");
        let next = fixed.unwrap_or("");
        let second = fixed.unwrap_or("second-session");
        let (traces, _) = execute(
            fixed,
            vec![
                Session {
                    steps: vec![
                        down(audio(Some("first-session"), vec![1, 0])),
                        reply("first-session-result", true, first, 1),
                    ],
                    end_label: None,
                },
                Session {
                    steps: vec![
                        reply("new-setup-before-audio", true, next, 0),
                        down(audio(Some("second-session"), vec![2, 0])),
                        reply("second-session-result", true, second, 1),
                    ],
                    end_label: None,
                },
            ],
        )
        .await;
        assert_eq!(traces[0].wire, vec![audio_wire(&[1, 0])]);
        assert_eq!(traces[1].wire, vec![audio_wire(&[2, 0])]);
    }
}

#[tokio::test]
async fn end_drain_retains_latest_label_until_real_final_result() {
    let (traces, observed) = execute(
        None,
        vec![Session {
            steps: vec![
                down(audio(Some("wire-speaker"), vec![1, 0])),
                reply("before-end-interim", false, "wire-speaker", 1),
                down(audio(Some("latest-empty-pcm"), vec![])),
            ],
            end_label: Some("latest-empty-pcm"),
        }],
    )
    .await;
    assert_eq!(
        traces[0].wire,
        vec![
            audio_wire(&[1, 0]),
            json!({"realtimeInput":{"audioStreamEnd":true}})
        ]
    );
    let final_index = observed[0].seen.iter().position(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "end-drain-0" && t.user_id == "latest-empty-pcm")).unwrap();
    let end_index = observed[0]
        .seen
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(
        final_index < end_index,
        "final transcript must drain before the End outlet"
    );
}
