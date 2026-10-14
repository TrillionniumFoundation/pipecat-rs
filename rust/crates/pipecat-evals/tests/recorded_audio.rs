//! Real WS -> RTVIProcessor -> PCM fixture -> native observer -> EvalSession.
//! The fixture recognizes exact sample bytes, not human speech or reference text.
use pipecat_core::*;
use pipecat_evals::{Cancellation, EvalSession, RecordedAudio, ScenarioFile, SessionOptions};
use pipecat_protocol::rtvi::{ClientCommand, Envelope};
use pipecat_rtvi::{Observation, RtviConfig, RtviObserver, RtviProcessor};
use pipecat_transports::{Packet, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Notify, task::JoinHandle, time::Instant};
type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(5);
fn options() -> SessionOptions {
    SessionOptions {
        default_timeout: Duration::from_secs(2),
        handshake_timeout: Duration::from_secs(2),
        max_duration: Duration::from_secs(4),
        stop_bot: true,
        transport: pipecat_transports::SessionConfig {
            close_timeout: Duration::from_millis(100),
            write_timeout: Duration::from_millis(300),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn scenario(expect: Value) -> pipecat_evals::ScriptScenario {
    ScenarioFile::from_value(
        json!({"name":"recording","turns":[{"user":"REFERENCE MUST NOT BE SENT","expect":expect}]}),
    )
    .unwrap()
    .scenarios
    .remove(0)
}
fn pcm(samples: usize, value: i16) -> Vec<u8> {
    (0..samples).flat_map(|_| value.to_le_bytes()).collect()
}
#[derive(Default, Debug)]
struct Trace {
    wire: Vec<(Instant, Vec<u8>)>,
    decoded: Vec<Vec<u8>>,
    text: usize,
    recognized: usize,
    report: Option<WorkerReport>,
}
struct Peer {
    url: String,
    task: JoinHandle<Result<Trace, Error>>,
    first: Arc<Notify>,
}
async fn peer(expected: Vec<u8>, early: bool, disconnect: bool) -> Peer {
    peer_many(vec![expected], early, disconnect, false).await
}
async fn peer_many(
    expected: Vec<Vec<u8>>,
    early: bool,
    disconnect: bool,
    allow_abrupt_drop: bool,
) -> Peer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let first = Arc::new(Notify::new());
    let notify = first.clone();
    let task = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(WAIT, listener.accept()).await??;
        let mut socket = WebSocketSession::accept(stream, options().transport).await?;
        let rtvi = Arc::new(RtviProcessor::new(RtviConfig::default())?);
        let handle = rtvi.handle();
        let observer = RtviObserver::new(Default::default(), None)?;
        let mut worker = Pipeline::new(vec![rtvi])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_millis(300),
                ..Default::default()
            })
            .await?;
        let mut trace = Trace::default();
        let mut heard = Vec::new();
        let mut answered = false;
        let run=tokio::time::timeout(WAIT,async {
            handle.set_bot_ready(None)?;
            loop {
                tokio::select! {
                    packet=socket.recv()=> {
                        let Some(Packet::Text(text))=packet else {break;};
                        let e=Envelope::parse(text.as_bytes())?;
                        if e.message_type=="client-message" && e.data.as_ref().is_some_and(|d|d["t"]=="eval-cancel") {break;}
                        if e.message_type=="send-text" {trace.text+=1;}
                        if let ClientCommand::RawAudio(audio)=e.client_command()? {
                            let bytes=audio.decode()?;
                            if audio.sample_rate!=16000 || audio.num_channels!=1 {return Err("bad wire format".into());}
                            trace.wire.push((Instant::now(),bytes)); notify.notify_one();
                            if disconnect {break;}
                        }
                        worker.sender().send(Frame::new(FrameData::InputTransportMessage(serde_json::to_value(e)?)),Direction::Downstream).await?;
                    }
                    frame=worker.recv()=> {
                        let Some(frame)=frame else {return Err("pipeline unexpectedly closed".into());};
                        match frame.frame.data {
                            FrameData::OutputTransportMessageUrgent(v)|FrameData::OutputTransportMessage(v)=>socket.sender().send(Packet::Text(v.to_string())).await?,
                            FrameData::InputAudio(a)=> {
                                trace.decoded.push(a.audio.clone());
                                let silent=a.audio.iter().all(|b|*b==0);
                                if answered && silent { answered=false; heard.clear(); }
                                if !silent && !answered {heard.extend_from_slice(&a.audio);}
                                if !answered && trace.recognized < expected.len() && ((early && !silent) || (silent && !heard.is_empty())) {
                                    let expected = &expected[trace.recognized];
                                    let valid=heard.starts_with(expected) && heard[expected.len().min(heard.len())..].iter().all(|b|*b==0);
                                    if early || valid {
                                        answered=early; trace.recognized+=1;
                                        heard.clear();
                                        for data in [FrameData::Transcription(TranscriptionFrame {text:format!("recorded tone {}",trace.recognized),finalized:true,..Default::default()}),FrameData::LlmResponseStart,FrameData::LlmText(TextFrame::new(format!("accepted PCM {}",trace.recognized))),FrameData::LlmResponseEnd] {
                                            for event in observer.observe(&Frame::new(data),Observation::new("audio-fixture",Direction::Downstream))? {
                                                socket.sender().send(Packet::Text(event.encode()?)).await?;
                                            }
                                        }
                                    }
                                }
                            }
                            FrameData::Error(e)=>return Err(format!("pipeline error: {e:?}").into()),
                            _=>{}
                        }
                    }
                }
            }
            Ok::<(),Error>(())
        }).await;
        // Always settle both owners before assertions or error propagation.
        let end = tokio::time::timeout(Duration::from_millis(300), worker.end()).await;
        let mut cleanup = Vec::new();
        if !matches!(end, Ok(Ok(()))) {
            cleanup.push(format!("End: {end:?}"));
            let cancelled =
                tokio::time::timeout(Duration::from_millis(300), worker.cancel("fixture cleanup"))
                    .await;
            if !matches!(cancelled, Ok(Ok(()))) {
                cleanup.push(format!("cancel: {cancelled:?}"));
            }
        }
        let drain = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = worker.recv().await {
                if let FrameData::InputAudio(a) = frame.frame.data {
                    trace.decoded.push(a.audio);
                }
            }
        })
        .await;
        if drain.is_err() {
            cleanup.push("pipeline drain deadline".into());
            let cancelled =
                tokio::time::timeout(Duration::from_millis(300), worker.cancel("drain deadline"))
                    .await;
            if !matches!(cancelled, Ok(Ok(()))) {
                cleanup.push(format!("drain cancel: {cancelled:?}"));
            }
        }
        let joined = match tokio::time::timeout(Duration::from_secs(1), worker.join()).await {
            Ok(result) => result,
            Err(_) => {
                cleanup.push("pipeline join deadline".into());
                let cancelled = tokio::time::timeout(
                    Duration::from_millis(300),
                    worker.cancel("join deadline"),
                )
                .await;
                if !matches!(cancelled, Ok(Ok(()))) {
                    cleanup.push(format!("join cancel: {cancelled:?}"));
                }
                worker.join().await
            }
        };
        let finishing = socket.finish();
        tokio::pin!(finishing);
        let closed = match tokio::time::timeout(Duration::from_secs(1), &mut finishing).await {
            Ok(result) => result,
            Err(_) => {
                cleanup.push("socket finish deadline".into());
                finishing.await
            }
        };
        match joined {
            Ok(report) => trace.report = Some(report),
            Err(e) => cleanup.push(e.to_string()),
        }
        if let Err(e) = closed {
            // Only external run-future abortion permits this exact WebSocket
            // protocol reset. Deadlines/I/O/protocol errors otherwise fail.
            let expected_reset = allow_abrupt_drop
                && matches!(&e,
                pipecat_transports::TransportError::WebSocket(error)
                if error.to_string() == "WebSocket protocol error: Connection reset without closing handshake");
            if !expected_reset {
                cleanup.push(e.to_string());
            }
        }
        if !cleanup.is_empty() {
            return Err(format!("cleanup: {cleanup:?}").into());
        }
        run??;
        Ok(trace)
    });
    Peer { url, task, first }
}
async fn joined(mut task: JoinHandle<Result<Trace, Error>>) -> Trace {
    let result = tokio::time::timeout(WAIT, &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
        panic!("peer failed to settle");
    }
    let trace = result.unwrap().unwrap().unwrap();
    let report = trace.report.as_ref().unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    trace
}
fn admitted(trace: &Trace, expected: &[u8]) {
    let bytes: Vec<_> = trace.wire.iter().flat_map(|(_, b)| b.clone()).collect();
    assert!(
        bytes.windows(expected.len()).any(|w| w == expected),
        "recording absent from wire"
    );
    let decoded: Vec<_> = trace.decoded.iter().flatten().copied().collect();
    assert_eq!(
        decoded, bytes,
        "RTVI pipeline did not receive the actual wire PCM"
    );
    assert_eq!(trace.text, 0);
    assert!(trace.wire.iter().all(|(_, b)| b.len() == 1280));
}
#[test]
fn clip_and_turn_validation_happens_before_connect() {
    assert!(RecordedAudio::from_pcm16(&[]).is_err());
    assert!(RecordedAudio::from_pcm16(&[1]).is_err());
    assert!(RecordedAudio::from_pcm16(&vec![0; 1_920_002]).is_err());
    assert!(RecordedAudio::from_wav(b"not wav").is_err());
    let clip = RecordedAudio::from_pcm16(&[1, 0]).unwrap();
    let make = || EvalSession::new(scenario(json!([])), "ws://127.0.0.1:9", options()).unwrap();
    assert!(make()
        .with_recorded_audio_inputs([(1, clip.clone())])
        .is_err());
    assert!(make()
        .with_recorded_audio_inputs([(0, clip.clone()), (0, clip)])
        .is_err());
    let wav = pipecat_audio::pcm_to_wav(&pcm(20, 123), 16000, 1).unwrap();
    assert_eq!(
        RecordedAudio::from_wav(&wav).unwrap().pcm_bytes(),
        pcm(20, 123)
    );
}
#[tokio::test]
async fn recording_reaches_real_pipeline_then_silence_yields_assertions() {
    let bytes = pcm(1500, 1234);
    let p = peer(bytes.clone(), false, false).await;
    let s=EvalSession::new(scenario(json!([{"event":"user_transcription","text_contains":"recorded tone"},{"event":"llm_response","text_contains":"accepted PCM"}])),&p.url,options()).unwrap().with_recorded_audio_inputs([(0,RecordedAudio::from_pcm16(&bytes).unwrap())]).unwrap();
    let result = s.run(Cancellation::new()).await;
    let trace = joined(p.task).await;
    assert!(result.passed, "{:?}", result.failures);
    admitted(&trace, &bytes);
    assert_eq!(trace.recognized, 1);
    assert!(trace.wire.len() >= 4);
    assert!(
        trace
            .wire
            .windows(2)
            .all(|w| w[1].0.duration_since(w[0].0) >= Duration::from_millis(20)),
        "unpaced burst"
    );
}
#[tokio::test]
async fn empty_expectations_and_early_response_still_finish_clip() {
    for early in [false, true] {
        let bytes = pcm(2500, 4321);
        let p = peer(bytes.clone(), early, false).await;
        let expect = if early {
            json!([{"event":"llm_response","text_contains":"accepted PCM"}])
        } else {
            json!([])
        };
        let result = EvalSession::new(scenario(expect), &p.url, options())
            .unwrap()
            .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&bytes).unwrap())])
            .unwrap()
            .run(Cancellation::new())
            .await;
        let trace = joined(p.task).await;
        assert!(result.passed, "{:?}", result.failures);
        admitted(&trace, &bytes);
    }
}
#[tokio::test]
async fn cancellation_stops_long_recording_and_joins_owners() {
    let bytes = pcm(16000 * 3, 111);
    let p = peer(bytes.clone(), false, false).await;
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let first = p.first.clone();
    let session = EvalSession::new(
        scenario(json!([{"event":"llm_response"}])),
        &p.url,
        options(),
    )
    .unwrap()
    .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&bytes).unwrap())])
    .unwrap();
    let trigger = tokio::spawn(async move {
        tokio::time::timeout(WAIT, first.notified()).await.unwrap();
        c.cancel();
    });
    let result = session.run(cancel).await;
    let triggered = trigger.await;
    let trace = joined(p.task).await;
    triggered.unwrap();
    assert!(!result.passed);
    assert!(result.failures.iter().any(|f| f.kind == "cancelled"));
    assert!(trace.wire.len() < 75);
}
#[tokio::test]
async fn idle_traffic_limit_and_peer_disconnect_are_failures() {
    for disconnect in [false, true] {
        let bytes = pcm(640, 222);
        let p = peer(pcm(640, 999), false, disconnect).await;
        let mut o = options();
        if !disconnect {
            o.max_wire_messages = 2;
        }
        let result = EvalSession::new(scenario(json!([{"event":"image"}])), &p.url, o)
            .unwrap()
            .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&bytes).unwrap())])
            .unwrap()
            .run(Cancellation::new())
            .await;
        let trace = joined(p.task).await;
        assert!(!result.passed);
        assert!(!trace.wire.is_empty());
        if !disconnect {
            assert!(result
                .failures
                .iter()
                .any(|f| f.reason.contains("recording wire traffic")));
        }
    }
}
#[tokio::test]
async fn dropping_run_aborts_audio_sender_and_closes_peer() {
    let bytes = pcm(16000 * 3, 333);
    let p = peer_many(vec![bytes.clone()], false, false, true).await;
    let session = EvalSession::new(
        scenario(json!([{"event":"llm_response"}])),
        &p.url,
        options(),
    )
    .unwrap()
    .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&bytes).unwrap())])
    .unwrap();
    let running = tokio::spawn(session.run(Cancellation::new()));
    let received = tokio::time::timeout(WAIT, p.first.notified()).await;
    running.abort();
    let stopped = running.await;
    let trace = joined(p.task).await;
    received.unwrap();
    assert!(stopped.unwrap_err().is_cancelled());
    assert!(trace.wire.len() < 75);
}

#[test]
fn unsupported_wav_and_aggregate_inputs_rejected_without_network() {
    for (rate, channels) in [(8000, 1), (16000, 2)] {
        let wav = pipecat_audio::pcm_to_wav(&pcm(20, 123), rate, channels).unwrap();
        assert!(RecordedAudio::from_wav(&wav).is_err());
    }
    let clip = RecordedAudio::from_pcm16(&[1, 0]).unwrap();
    for turn in [json!({}), json!({"user":" "}), json!({"dtmf":"1"})] {
        let scenario = ScenarioFile::from_value(json!({"name":"invalid","turns":[turn]}))
            .unwrap()
            .scenarios
            .remove(0);
        assert!(EvalSession::new(scenario, "ws://127.0.0.1:9", options())
            .unwrap()
            .with_recorded_audio_inputs([(0, clip.clone())])
            .is_err());
    }
    let make = |count: usize| {
        let turns: Vec<_> = (0..count).map(|_| json!({"user":"reference"})).collect();
        let scenario = ScenarioFile::from_value(json!({"name":"limit","turns":turns}))
            .unwrap()
            .scenarios
            .remove(0);
        EvalSession::new(scenario, "ws://127.0.0.1:9", options()).unwrap()
    };
    assert!(make(129)
        .with_recorded_audio_inputs((0..129).map(|i| (i, clip.clone())))
        .is_err());
    let large = RecordedAudio::from_pcm16(&vec![0; 1_920_000]).unwrap();
    assert!(make(9)
        .with_recorded_audio_inputs((0..9).map(|i| (i, large.clone())))
        .is_err());
    assert!(make(8)
        .with_recorded_audio_inputs((0..8).map(|i| (i, large.clone())))
        .is_ok());
}
#[tokio::test]
async fn two_distinct_recordings_keep_turns_and_reference_text_separate() {
    let first = pcm(1500, 1234);
    let second = pcm(2000, -2345);
    let p = peer_many(vec![first.clone(), second.clone()], false, false, false).await;
    let turns:Vec<_>=(1..=2).map(|n|json!({"user":format!("PRIVATE REFERENCE {n}"),"expect":[{"event":"user_transcription","text_contains":format!("recorded tone {n}")},{"event":"llm_response","text_contains":format!("accepted PCM {n}")}]})).collect();
    let scenario = ScenarioFile::from_value(json!({"name":"two recordings","turns":turns}))
        .unwrap()
        .scenarios
        .remove(0);
    let result = EvalSession::new(scenario, &p.url, options())
        .unwrap()
        .with_recorded_audio_inputs([
            (0, RecordedAudio::from_pcm16(&first).unwrap()),
            (1, RecordedAudio::from_pcm16(&second).unwrap()),
        ])
        .unwrap()
        .run(Cancellation::new())
        .await;
    let trace = joined(p.task).await;
    assert!(result.passed, "{:?}", result.failures);
    admitted(&trace, &first);
    admitted(&trace, &second);
    assert_eq!(trace.recognized, 2);
}

#[tokio::test]
async fn expectation_deadline_starts_at_queue_not_after_long_recording() {
    let bytes = pcm(16000 * 3, 444);
    let p = peer(bytes.clone(), false, false).await;
    let session = EvalSession::new(
        scenario(json!([{"event":"llm_response","within_ms":20}])),
        &p.url,
        options(),
    )
    .unwrap()
    .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&bytes).unwrap())])
    .unwrap();
    let result = session.run(Cancellation::new()).await;
    let trace = joined(p.task).await;
    assert!(!result.passed);
    assert!(result.failures.iter().any(|f| f.kind == "timeout"));
    assert!(trace.wire.len() < 75, "deadline waited for full recording");
}

#[test]
fn fixed_source_raw_audio_vectors_match_reused_native_codec() {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/recorded_audio_source.json")).unwrap();
    assert_eq!(
        source["revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    let mut codec = pipecat_protocol::rtvi::RtviClientCodec::default();
    for vector in source["serializer_vectors"].as_array().unwrap() {
        let hex = vector["input_pcm_hex"].as_str().unwrap();
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let wire: Value =
            serde_json::from_str(&codec.encode_audio(&bytes, 16000, 1).unwrap()).unwrap();
        assert_eq!(wire, vector["envelope"]);
    }
}

#[tokio::test]
async fn default_and_empty_recording_map_keep_text_wire_without_silence() {
    for empty_builder in [false, true] {
        let p = peer(vec![1, 0], false, false).await;
        let session = EvalSession::new(scenario(json!([])), &p.url, options()).unwrap();
        let session = if empty_builder {
            session.with_recorded_audio_inputs([]).unwrap()
        } else {
            session
        };
        let result = session.run(Cancellation::new()).await;
        let trace = joined(p.task).await;
        assert!(result.passed, "{:?}", result.failures);
        assert_eq!(trace.text, 1);
        assert!(trace.wire.is_empty());
    }
}
