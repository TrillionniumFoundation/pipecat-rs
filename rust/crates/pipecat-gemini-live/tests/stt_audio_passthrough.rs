//! Dedicated STT audio routing changes only original-frame forwarding.
//! Real localhost WebSocket + production Pipeline; no hosted-provider claim.
//! Same-lane processed text, actual transcripts/errors and completed owner joins
//! establish observations. WS Pong is protocol housekeeping only.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::tungstenite::Message;

// Preserve the complete affected graph once: metadata (6), speaker labels (5),
// audio pause (5), live (28), STT mute (4), input audio families (13): 61 tests.
#[path = "stt_metadata.rs"]
mod stt_metadata_regressions;

const WAIT: Duration = Duration::from_secs(5);

enum Step {
    Frame(Box<Frame>, Direction),
    Mute(bool, Direction),
    Pause(bool),
    WaitReply {
        text: String,
        label: &'static str,
    },
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
struct Original {
    frame: Frame,
    tag: LocalFrameTag,
    direction: Direction,
    forwarded: bool,
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
fn decorate(mut frame: Frame) -> Result<(Frame, LocalFrameTag), String> {
    frame.name = "stt-audio-route-original-envelope".into();
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
    Ok((frame, tag))
}
async fn original(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    originals: &mut Vec<Original>,
    frame: Frame,
    direction: Direction,
    forwarded: bool,
) -> Result<(), String> {
    let (frame, tag) = decorate(frame)?;
    let expected = frame.clone();
    worker
        .sender()
        .send(frame, direction)
        .await
        .map_err(|e| e.to_string())?;
    originals.push(Original {
        frame: expected.clone(),
        tag,
        direction,
        forwarded,
    });
    if forwarded {
        while !seen
            .iter()
            .any(|f| f.frame.id == expected.id && f.direction == direction)
        {
            next(worker, seen).await?;
        }
    } else {
        // Ordinary text follows audio in this same runtime lane. Its actual
        // outlet proves Gemini's audio invocation returned; queue admission,
        // observer callbacks and protocol Ping/Pong do not establish this.
        fence(worker, seen, direction).await?;
        if seen.iter().any(|f| f.frame.id == expected.id) {
            return Err("suppressed original audio reached an outlet".into());
        }
    }
    Ok(())
}
async fn fence(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    direction: Direction,
) -> Result<(), String> {
    let (frame, tag) = decorate(Frame::text("audio-route-processed-fence"))?;
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
                return Err("processed fence envelope changed".into());
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
async fn execute(
    mode: SessionMode,
    passthrough: Option<bool>,
    fixed: Option<&str>,
    sessions: Vec<Session>,
) -> (Vec<Trace>, Vec<Observed>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = if mode == SessionMode::Transcription {
        GeminiConfig::transcription("audio-route-key", "audio-route-model")
    } else {
        GeminiConfig::new("audio-route-key", "audio-route-model")
    };
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    config.limits.max_audio_bytes = 32;
    config.limits.finish_timeout = Duration::from_secs(2);
    let mut processor = GeminiLiveProcessor::new(GeminiClient::new(config).unwrap());
    if let Some(enabled) = passthrough {
        processor = processor.with_audio_passthrough(enabled);
    }
    if let Some(user_id) = fixed {
        processor = processor.with_user_id(user_id);
    }
    let processor = Arc::new(processor);
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
            let mut flush_count = 0;
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
                            // A real VAD stop or End flush unlocks this final result.
                            flush_count += 1;
                            let r = Reply {
                                text: format!("stream-end-{session}-{flush_count}"),
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
                } => Some((
                    text.clone(),
                    *finalized && mode == SessionMode::Transcription,
                    *label,
                )),
                Step::WaitReply { text, label } => Some((text.clone(), true, *label)),
                _ => None,
            })
            .collect();
        if let Some(label) = session.end_label {
            expected_replies.push((format!("stream-end-{session_index}-1"), true, label));
        }
        let mut seen = vec![];
        let mut originals = vec![];
        let mut rejected_ids = vec![];
        let mut consumed_ids = vec![];
        let observed = timeout(WAIT, async {
            while !seen.iter().any(|f: &DirectedFrame| f.frame.data.is_start()) {
                next(&mut worker, &mut seen).await?;
            }
            fence(&mut worker, &mut seen, Direction::Downstream).await?;
            fence(&mut worker, &mut seen, Direction::Upstream).await?;
            for step in session.steps {
                match step {
                    Step::Frame(frame, direction) => {
                        let is_audio = matches!(
                            frame.data,
                            FrameData::InputAudio(_) | FrameData::UserAudio(_)
                        );
                        let forwarded = !(mode == SessionMode::Transcription
                            && passthrough == Some(false)
                            && is_audio);
                        original(
                            &mut worker,
                            &mut seen,
                            &mut originals,
                            *frame,
                            direction,
                            forwarded,
                        )
                        .await?;
                    }
                    Step::Pause(paused) => processor.set_audio_input_paused(paused),
                    Step::WaitReply { text, label } => {
                        wait_reply(&mut worker, &mut seen, &text, true, label).await?;
                    }
                    Step::Mute(muted, direction) => {
                        let frame = Frame::new(FrameData::SttMute(muted));
                        let id = frame.id;
                        consumed_ids.push(id);
                        worker
                            .sender()
                            .send(frame, direction)
                            .await
                            .map_err(|e| e.to_string())?;
                        // Normal same-lane work waits for queued/in-flight System mute.
                        fence(&mut worker, &mut seen, direction).await?;
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
                        // Issued after an original outlet, same-lane processed fence
                        // or actual error witness. Wire receipt alone is not a label fence.
                        commands
                            .send(Reply {
                                text: text.clone(),
                                finalized,
                                after_audio,
                            })
                            .await
                            .map_err(|e| e.to_string())?;
                        // Conversation keeps input transcription interim even when
                        // the wire response completes the provider turn.
                        let expected_final = finalized && mode == SessionMode::Transcription;
                        wait_reply(&mut worker, &mut seen, &text, expected_final, label).await?;
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
                worker.cancel("audio-route observation failed").await
            }
        })
        .await;
        if !matches!(admitted, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admitted:?}"));
            let _ = timeout(WAIT, worker.cancel("audio-route terminal cleanup")).await;
        }
        let joined = match timeout(WAIT, drain_join(&mut worker, &mut seen)).await {
            Ok(report) => report,
            Err(error) => {
                failures.push(format!("owner join deadline: {error}"));
                let _ = timeout(WAIT, worker.cancel("audio-route join cleanup")).await;
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
                        || seen.iter().any(|f| consumed_ids.contains(&f.frame.id))
                    {
                        failures.push(format!("unexpected final owner report: {report:?}"));
                    }
                    for expected in &originals {
                        let actual: Vec<_> = seen
                            .iter()
                            .filter(|f| f.frame.id == expected.frame.id)
                            .collect();
                        if expected.forwarded {
                            if actual.len() != 1
                                || actual[0].direction != expected.direction
                                || actual[0].frame != expected.frame
                                || !expected.tag.contains(&actual[0].frame)
                            {
                                failures.push(format!(
                                    "forwarded original changed/duplicated: {actual:?}"
                                ));
                            }
                        } else if !actual.is_empty() {
                            failures.push(format!("suppressed original leaked: {actual:?}"));
                        }
                    }
                    let expected_audio_count = originals
                        .iter()
                        .filter(|original| {
                            original.forwarded
                                && matches!(
                                    original.frame.data,
                                    FrameData::InputAudio(_) | FrameData::UserAudio(_)
                                )
                        })
                        .count();
                    let actual_audio_count = seen
                        .iter()
                        .filter(|output| {
                            matches!(
                                output.frame.data,
                                FrameData::InputAudio(_) | FrameData::UserAudio(_)
                            )
                        })
                        .count();
                    if actual_audio_count != expected_audio_count {
                        failures.push(format!("unexpected audio outlets, including rewritten IDs: {actual_audio_count}; expected {expected_audio_count}"));
                    }
                    if mode == SessionMode::Transcription
                        && seen
                            .iter()
                            .any(|output| matches!(output.frame.data, FrameData::SttMute(_)))
                    {
                        failures.push("STT mute control escaped consumption".into());
                    }
                    let outlet_errors: Vec<_> = seen
                        .iter()
                        .filter_map(|f| {
                            if let FrameData::Error(error) = &f.frame.data {
                                Some(error.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    if outlet_errors != report.errors {
                        failures.push("error journal differs from actual error outlets".into());
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
        assert_eq!(trace.setup["setup"]["model"], "models/audio-route-model");
        assert!(trace.wire.iter().all(|value| value.get("setup").is_none()));
    }
    (traces, observations)
}

fn upstream(frame: Frame) -> Step {
    Step::Frame(Box::new(frame), Direction::Upstream)
}
fn malformed(user: Option<&str>) -> Frame {
    wrapped(
        user,
        AudioRawFrame {
            audio: vec![1, 2, 3],
            sample_rate: 0,
            num_channels: 0,
            context_id: None,
        },
    )
}
fn stopped(detailed: bool) -> Frame {
    Frame::new(if detailed {
        FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame::default())
    } else {
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        })
    })
}
fn wait_flush(session: usize, ordinal: usize, label: &'static str) -> Step {
    Step::WaitReply {
        text: format!("stream-end-{session}-{ordinal}"),
        label,
    }
}
fn stream_end_wire() -> Value {
    json!({"realtimeInput":{"audioStreamEnd":true}})
}
fn assert_end_after_final(observed: &Observed, text: &str, label: &str) {
    let final_index = observed
        .seen
        .iter()
        .position(|f| {
            matches!(&f.frame.data,
        FrameData::Transcription(t) if t.text == text && t.user_id == label)
        })
        .unwrap();
    let end_index = observed
        .seen
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(
        final_index < end_index,
        "real final must drain before the End outlet"
    );
}

#[tokio::test]
async fn default_and_true_preserve_originals_false_keeps_identical_provider_work_and_transcripts() {
    let mut baseline = None;
    for passthrough in [None, Some(true), Some(false)] {
        let (traces, observed) = execute(
            SessionMode::Transcription,
            passthrough,
            None,
            vec![Session {
                steps: vec![
                    reply("before-audio", true, "", 0),
                    down(audio(Some("alice"), vec![1, 0])),
                    reply("user-interim", false, "alice", 1),
                    down(audio(None, vec![2, 0])),
                    reply("plain-final", true, "", 2),
                    upstream(audio(Some("upstream-user"), vec![3, 0])),
                    upstream(audio(None, vec![4, 0])),
                    reply("after-upstream", false, "", 2),
                    down(audio(Some("empty-user"), vec![])),
                    reply("empty-user-final", true, "empty-user", 2),
                    down(Frame::new(FrameData::OutputAudio(
                        AudioRawFrame::new(vec![9, 0], 24_000, 1).unwrap(),
                    ))),
                    upstream(Frame::new(FrameData::OutputAudio(
                        AudioRawFrame::new(vec![10, 0], 24_000, 1).unwrap(),
                    ))),
                    down(Frame::new(FrameData::TtsAudio(
                        AudioRawFrame::new(vec![11, 0], 24_000, 1).unwrap(),
                    ))),
                    upstream(Frame::new(FrameData::TtsAudio(
                        AudioRawFrame::new(vec![12, 0], 24_000, 1).unwrap(),
                    ))),
                    down(Frame::text("non-audio-text")),
                    upstream(Frame::text("upstream-non-audio-text")),
                    down(Frame::new(FrameData::UserStartedSpeaking)),
                    down(Frame::new(FrameData::UserStoppedSpeaking)),
                ],
                end_label: None,
            }],
        )
        .await;
        assert_eq!(
            traces[0].wire,
            vec![audio_wire(&[1, 0]), audio_wire(&[2, 0])]
        );
        let transcripts: Vec<_> = observed[0]
            .seen
            .iter()
            .filter_map(transcript)
            .map(|t| {
                (
                    t.text.clone(),
                    t.finalized,
                    t.user_id.clone(),
                    t.language.clone(),
                )
            })
            .collect();
        let comparable = (traces[0].setup.clone(), traces[0].wire.clone(), transcripts);
        if let Some(expected) = &baseline {
            assert_eq!(&comparable, expected);
        } else {
            baseline = Some(comparable);
        }
    }
}

#[tokio::test]
async fn false_filters_after_mute_empty_and_validation_semantics_in_both_directions() {
    let mut steps = vec![
        down(audio(Some("accepted"), vec![1, 0])),
        reply("accepted-interim", false, "accepted", 1),
        Step::Mute(true, Direction::Downstream),
    ];
    for user in [None, Some("muted-speaker")] {
        steps.push(down(audio(user, vec![2, 0])));
        steps.push(down(audio(user, vec![])));
        // Muted invalid PCM is neither validated nor allowed to change the label.
        steps.push(down(malformed(user)));
    }
    steps.push(reply("muted-final", true, "accepted", 1));
    steps.push(Step::Mute(false, Direction::Upstream));
    for user in [None, Some("upstream-speaker")] {
        steps.push(upstream(audio(user, vec![3, 0])));
        steps.push(upstream(audio(user, vec![])));
        steps.push(upstream(malformed(user)));
    }
    steps.push(reply("upstream-final", true, "accepted", 1));
    steps.push(down(audio(Some("empty-user"), vec![])));
    steps.push(reply("empty-user-interim", false, "empty-user", 1));
    for user in [None, Some("invalid-speaker")] {
        for case in 0..4 {
            let mut pcm = AudioRawFrame::new(vec![4, 0], 16_000, 1).unwrap();
            let (category, message) = match case {
                0 => {
                    pcm.audio.push(3);
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                1 => {
                    pcm.sample_rate = 8_000;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
                2 => {
                    pcm.audio = vec![0; 34];
                    (ErrorCategory::Application, "configured limit")
                }
                _ => {
                    pcm.audio.clear();
                    pcm.sample_rate = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
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
                "empty-user",
                1,
            ));
        }
    }
    // Enqueue rejection is after the mapping label update, unlike validation failure.
    for (user, label) in [(Some("enqueue-label"), "enqueue-label"), (None, "")] {
        let mut pcm = AudioRawFrame::new(vec![5, 0], 16_000, 1).unwrap();
        pcm.context_id = Some("x".repeat(1025));
        steps.push(Step::Rejected(
            Box::new(wrapped(user, pcm)),
            ErrorCategory::Application,
            "configured limit",
        ));
        steps.push(reply(format!("enqueue-{user:?}"), true, label, 1));
    }
    steps.extend([
        down(audio(Some("latest-empty"), vec![])),
        reply("latest-empty-interim", false, "latest-empty", 1),
        down(audio(None, vec![])),
        reply("plain-empty-interim", false, "", 1),
        down(audio(Some("resumed"), vec![6, 0])),
        reply("resumed-interim", false, "resumed", 2),
        upstream(stopped(false)),
        upstream(stopped(true)),
        down(stopped(false)),
        wait_flush(0, 1, "resumed"),
        down(stopped(true)),
        wait_flush(0, 2, "resumed"),
        down(Frame::new(FrameData::UserStartedSpeaking)),
        down(Frame::new(FrameData::UserStoppedSpeaking)),
    ]);
    let (traces, observed) = execute(
        SessionMode::Transcription,
        Some(false),
        None,
        vec![Session {
            steps,
            end_label: None,
        }],
    )
    .await;
    assert_eq!(
        traces[0].wire,
        vec![
            audio_wire(&[1, 0]),
            audio_wire(&[6, 0]),
            stream_end_wire(),
            stream_end_wire()
        ]
    );
    assert_eq!(observed[0].report.errors.len(), 10);
}

#[tokio::test]
async fn conversation_ignores_the_flag_and_keeps_paused_audio_and_control_passthrough() {
    let mut baseline = None;
    for passthrough in [None, Some(true), Some(false)] {
        let (traces, _) = execute(
            SessionMode::Conversation,
            passthrough,
            None,
            vec![Session {
                steps: vec![
                    down(audio(Some("conversation-user"), vec![1, 0])),
                    reply("conversation-user-interim", false, "", 1),
                    down(audio(None, vec![2, 0])),
                    reply("conversation-plain-final", true, "", 2),
                    upstream(audio(Some("upstream-user"), vec![3, 0])),
                    upstream(audio(None, vec![4, 0])),
                    down(Frame::new(FrameData::SttMute(true))),
                    upstream(Frame::new(FrameData::SttMute(false))),
                    Step::Pause(true),
                    down(audio(Some("paused-user"), vec![5, 0])),
                    down(audio(None, vec![6, 0])),
                    down(malformed(Some("paused-invalid"))),
                    down(malformed(None)),
                    Step::Pause(false),
                    down(audio(Some("resumed-user"), vec![7, 0])),
                    down(audio(None, vec![8, 0])),
                    reply("conversation-resumed-final", true, "", 4),
                    down(stopped(false)),
                    down(stopped(true)),
                    down(Frame::text("ordinary-text")),
                ],
                end_label: None,
            }],
        )
        .await;
        let expected = vec![
            audio_wire(&[1, 0]),
            audio_wire(&[2, 0]),
            audio_wire(&[7, 0]),
            audio_wire(&[8, 0]),
        ];
        assert_eq!(traces[0].wire, expected);
        let comparable = (traces[0].setup.clone(), traces[0].wire.clone());
        if let Some(expected) = &baseline {
            assert_eq!(&comparable, expected);
        } else {
            baseline = Some(comparable);
        }
    }
}

#[tokio::test]
async fn end_flushes_previously_sent_pcm_but_never_empty_or_muted_only_input() {
    for passthrough in [None, Some(true), Some(false)] {
        let (traces, observed) = execute(
            SessionMode::Transcription,
            passthrough,
            None,
            vec![
                Session {
                    steps: vec![
                        down(audio(Some("sent-speaker"), vec![1, 0])),
                        reply("pending-interim", false, "sent-speaker", 1),
                        down(audio(Some("latest-empty"), vec![])),
                        Step::Mute(true, Direction::Downstream),
                        down(audio(None, vec![2, 0])),
                        down(malformed(Some("muted-after-send"))),
                    ],
                    end_label: Some("latest-empty"),
                },
                Session {
                    steps: vec![
                        down(audio(None, vec![])),
                        down(audio(Some("only-empty"), vec![])),
                        reply("only-empty-interim", false, "only-empty", 0),
                    ],
                    end_label: None,
                },
                Session {
                    steps: vec![
                        Step::Mute(true, Direction::Upstream),
                        down(audio(Some("only-muted-user"), vec![3, 0])),
                        down(audio(None, vec![4, 0])),
                        down(malformed(Some("only-muted-invalid"))),
                        down(malformed(None)),
                        reply("only-muted-interim", false, "", 0),
                    ],
                    end_label: None,
                },
            ],
        )
        .await;
        assert_eq!(traces[0].wire, vec![audio_wire(&[1, 0]), stream_end_wire()]);
        assert_end_after_final(&observed[0], "stream-end-0-1", "latest-empty");
        assert!(traces[1].wire.is_empty());
        assert!(traces[2].wire.is_empty());
        assert!(observed[1..].iter().all(|o| o
            .seen
            .iter()
            .filter_map(transcript)
            .all(|t| !t.finalized)));
    }
}

#[tokio::test]
async fn reused_processor_retains_routing_and_fixed_labels_but_resets_dynamic_state() {
    for passthrough in [Some(false), Some(true)] {
        for fixed in [None, Some("configured-speaker"), Some("")] {
            let first_label = fixed.unwrap_or("first-session");
            let fresh_label = fixed.unwrap_or("");
            let second_label = fixed.unwrap_or("second-session");
            let (traces, observed) = execute(
                SessionMode::Transcription,
                passthrough,
                fixed,
                vec![
                    Session {
                        steps: vec![
                            down(audio(Some("first-session"), vec![1, 0])),
                            reply("first-interim", false, first_label, 1),
                            Step::Mute(true, Direction::Downstream),
                            down(audio(Some("muted-first"), vec![2, 0])),
                        ],
                        end_label: Some(first_label),
                    },
                    Session {
                        steps: vec![
                            reply("fresh-setup-final", true, fresh_label, 0),
                            down(audio(Some("second-session"), vec![3, 0])),
                            reply("second-interim", false, second_label, 1),
                            upstream(audio(Some("upstream-second"), vec![4, 0])),
                            down(audio(None, vec![5, 0])),
                            reply("second-plain-final", true, fresh_label, 2),
                        ],
                        end_label: None,
                    },
                ],
            )
            .await;
            assert_eq!(traces[0].wire, vec![audio_wire(&[1, 0]), stream_end_wire()]);
            assert_eq!(
                traces[1].wire,
                vec![audio_wire(&[3, 0]), audio_wire(&[5, 0])]
            );
            assert_end_after_final(&observed[0], "stream-end-0-1", first_label);
        }
    }
}
