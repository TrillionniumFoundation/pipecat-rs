//! Dedicated STT mute routing and WebSocket admission; no provider credentials.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;
const WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq)]
enum Expect {
    Forward,
    Consumed,
    Final,
}
type Step = (Frame, Direction, Expect);
fn down(frame: Frame) -> Step {
    (frame, Direction::Downstream, Expect::Forward)
}
fn mute(value: bool, direction: Direction) -> Step {
    (
        Frame::new(FrameData::SttMute(value)),
        direction,
        Expect::Consumed,
    )
}
fn finalize() -> Step {
    (
        Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        })),
        Direction::Downstream,
        Expect::Final,
    )
}
fn audio(bytes: Vec<u8>, user: bool) -> Frame {
    let audio = AudioRawFrame::new(bytes, 16_000, 1).unwrap();
    Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "speaker".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    })
}
fn start() -> Step {
    down(Frame::new(FrameData::UserStartedSpeaking))
}
fn stop() -> Step {
    down(Frame::new(FrameData::UserStoppedSpeaking))
}
fn config(manual: bool) -> GeminiConfig {
    let mut config = GeminiConfig::new("preroll-fixture-key", "preroll-model");
    config.session.activity_detection.disabled = manual;
    config
}
fn pcm(trace: &[Value]) -> Vec<Vec<u8>> {
    trace
        .iter()
        .filter_map(|v| {
            v["realtimeInput"]["audio"]["data"]
                .as_str()
                .map(|s| STANDARD.decode(s).unwrap())
        })
        .collect()
}

async fn execute(
    mut config: GeminiConfig,
    fixed: Option<u32>,
    sessions: Vec<Vec<Step>>,
) -> Vec<Vec<Value>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let count = sessions.len();
    let transcription = config.mode == SessionMode::Transcription;
    let mut peer = tokio::spawn(async move {
        let mut traces = Vec::new();
        for _ in 0..count {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "accept timeout")?
                .map_err(|e| e.to_string())?;
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            let setup = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "setup timeout")?
                .ok_or("missing setup")?
                .map_err(|e| e.to_string())?;
            let setup: Value = serde_json::from_str(setup.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if setup["setup"]["model"] != "models/preroll-model" {
                return Err("wrong setup".to_owned());
            }
            ws.send(Message::Text(
                json!({"setupComplete":{}}).to_string().into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
            let mut trace = Vec::new();
            loop {
                match timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "peer read timeout")?
                {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        let input = &value["realtimeInput"];
                        let completes = input.get("audio").is_some()
                            || input.get("activityEnd").is_some()
                            || input.get("audioStreamEnd").is_some();
                        trace.push(value);
                        if completes {
                            let response = if transcription {
                                if trace.last().unwrap()["realtimeInput"]
                                    .get("audioStreamEnd")
                                    .is_some()
                                {
                                    json!({"serverContent":{"inputTranscription":{"text":"committed","languageCode":"en-US"},"turnComplete":true}})
                                } else {
                                    json!({"serverContent":{"interimInputTranscription":{"text":"pending","languageCode":"en-US"}}})
                                }
                            } else {
                                json!({"serverContent":{"turnComplete":true}})
                            };
                            ws.send(Message::Text(response.to_string().into()))
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => ws
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(|e| e.to_string())?,
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        let _ = ws.flush().await;
                        break;
                    }
                    other => return Err(format!("expected actual WebSocket close: {other:?}")),
                }
            }
            traces.push(trace);
        }
        Ok::<_, String>(traces)
    });
    let mut processor = GeminiLiveProcessor::new(GeminiClient::new(config).unwrap());
    if let Some(milliseconds) = fixed {
        processor = processor.with_preroll_ms(milliseconds).unwrap();
    }
    let processor = Arc::new(processor);
    let mut failures = Vec::new();
    for steps in sessions {
        let expected_errors = 0;
        let created = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(2),
                processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
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
                failures.push(error.to_string());
                break;
            }
        };
        let observed = timeout(WAIT, async {
            loop {
                let out = worker.recv().await.ok_or("closed before Start")?;
                if out.frame.data.is_start() {
                    break;
                }
            }
            for (mut frame, direction, expect) in steps {
                let tag = LocalFrameTag::new();
                tag.mark(&mut frame).map_err(|e| e.to_string())?;
                frame.name = "stt-mute-control-flow".into();
                frame
                    .metadata
                    .insert("witness".into(), json!({"preserved":true}));
                frame.set_pipeline_pts(1234);
                let mut expected = frame.clone();
                worker
                    .sender()
                    .send(frame, direction)
                    .await
                    .map_err(|e| e.to_string())?;
                if expect == Expect::Consumed {
                    expected = Frame::text("mute-observation-barrier");
                    worker
                        .sender()
                        .send(expected.clone(), direction)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                let mut forwarded = false;
                let mut final_seen = false;
                loop {
                    let out = worker.recv().await.ok_or("closed before forwarding")?;
                    if matches!(
                        out.frame.data,
                        FrameData::Error(_) | FrameData::ErrorDetailed(_)
                    ) {
                        return Err(format!("unexpected error: {:?}", out.frame.data));
                    }
                    if transcription && matches!(out.frame.data, FrameData::SttMute(_)) {
                        return Err("dedicated STT leaked consumed mute control".into());
                    }
                    if let FrameData::Transcription(t) = &out.frame.data {
                        if t.text != "committed" || !t.finalized {
                            return Err("wrong final transcript".into());
                        }
                        final_seen = true;
                    }
                    if out.frame.id == expected.id && out.direction == direction {
                        if expect != Expect::Consumed
                            && (!tag.contains(&out.frame)
                                || out.frame.name != expected.name
                                || out.frame.metadata != expected.metadata
                                || out.frame.pts != expected.pts)
                        {
                            return Err("forwarded envelope or local identity changed".into());
                        }
                        forwarded = true;
                    }
                    if forwarded && (expect != Expect::Final || final_seen) {
                        break;
                    }
                }
                if !worker
                    .processor_health(0)
                    .ok_or("missing health")?
                    .is_usable()
                    || !worker.unusable_processors().is_empty()
                {
                    return Err("mute or audio changed service usability".into());
                }
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "observation timeout".to_owned())
        .and_then(|r| r);
        let normal = observed.is_ok();
        if let Err(error) = observed {
            failures.push(error);
        }
        let admission = timeout(WAIT, async {
            if normal {
                worker.end().await
            } else {
                worker.cancel("STT mute observation failed").await
            }
        })
        .await;
        if !matches!(admission, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admission:?}"));
            let _ = worker.cancel("terminal deadline").await;
        }
        let joined = match timeout(WAIT, async {
            while let Some(out) = worker.recv().await {
                if transcription && matches!(out.frame.data, FrameData::SttMute(_)) {
                    failures.push("late leaked mute control".into());
                }
            }
            worker.join().await
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                failures.push(error.to_string());
                let _ = worker.cancel("join deadline").await;
                worker.join().await
            }
        };
        match joined {
            Ok(report) => {
                if report.total_errors != expected_errors
                    || (normal && report.state != WorkerState::Ended)
                {
                    failures.push(format!("unexpected report: {report:?}"));
                }
            }
            Err(error) => failures.push(error.to_string()),
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
            Err(format!("peer deadline, aborted and joined: {error}"))
        }
    };
    assert!(failures.is_empty(), "{failures:?}; peer={traces:?}");
    traces.unwrap()
}

#[tokio::test]
async fn dedicated_mute_both_directions_suppresses_only_new_pcm_and_keeps_finalization() {
    let a = vec![1, 0, 2, 0];
    let b = vec![3, 0, 4, 0];
    let c = vec![5, 0, 6, 0];
    let mut invalid = audio(vec![5, 0], false);
    if let FrameData::InputAudio(ref mut data) = invalid.data {
        data.num_channels = 0;
    }
    let trace = execute(
        GeminiConfig::transcription("fixture-key", "preroll-model"),
        None,
        vec![vec![
            down(audio(a.clone(), false)),
            mute(true, Direction::Downstream),
            down(audio(vec![9, 0], false)),
            down(audio(vec![8, 0], true)),
            down(invalid),
            down(audio(vec![7; 400_000], true)),
            finalize(), // Already admitted utterance finishes while still muted.
            (
                audio(vec![6, 0], true),
                Direction::Upstream,
                Expect::Forward,
            ),
            mute(false, Direction::Upstream),
            down(audio(b.clone(), true)),
            mute(true, Direction::Upstream),
            down(audio(vec![10, 0], false)),
            finalize(),
            mute(false, Direction::Downstream),
            down(audio(c.clone(), false)),
            finalize(),
        ]],
    )
    .await;
    assert_eq!(pcm(&trace[0]), vec![a, b, c]);
    assert_eq!(trace[0].len(), 6);
    for index in [1, 3, 5] {
        assert_eq!(
            trace[0][index],
            json!({"realtimeInput":{"audioStreamEnd":true}})
        );
    }
}
#[tokio::test]
async fn conversation_mute_controls_remain_forwarded_without_changing_audio() {
    for manual in [false, true] {
        let mut steps = vec![
            down(Frame::new(FrameData::SttMute(true))),
            (
                Frame::new(FrameData::SttMute(true)),
                Direction::Upstream,
                Expect::Forward,
            ),
        ];
        if manual {
            steps.push(start());
        }
        steps.push(down(audio(vec![1, 0, 2, 0], true)));
        if manual {
            steps.push(stop());
        }
        let trace = execute(config(manual), None, vec![steps]).await;
        assert_eq!(pcm(&trace[0]), vec![vec![1, 0, 2, 0]]);
        assert_eq!(trace[0].len(), if manual { 3 } else { 2 });
    }
}
#[tokio::test]
async fn completed_sequential_setup_resets_mute_to_unmuted() {
    let trace = execute(
        GeminiConfig::transcription("fixture-key", "preroll-model"),
        None,
        vec![
            vec![
                mute(true, Direction::Upstream),
                down(audio(vec![9, 0], true)),
            ],
            vec![down(audio(vec![1, 0], false)), finalize()],
        ],
    )
    .await;
    assert!(trace[0].is_empty());
    assert_eq!(pcm(&trace[1]), vec![vec![1, 0]]);
    assert_eq!(trace[1].len(), 2);
}

#[test]
fn pinned_source_contract_and_explicit_native_session_difference() {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/python_stt_mute_source.json")).unwrap();
    for name in ["mute_resume_downstream", "mute_resume_upstream"] {
        assert_eq!(source[name]["initial_muted"], false);
        assert_eq!(source[name]["after_mute"]["muted"], true);
        assert_eq!(source[name]["after_mute"]["provider"], json!([]));
        assert_eq!(
            source[name]["after_resume"]["provider"][0]["audio_hex"],
            "0200"
        );
    }
    assert_eq!(
        source["vad_while_muted"]["provider"],
        json!([{"audio_stream_end":true}])
    );
    assert_eq!(
        source["repeated_setup_source_preserves_mute"]["muted"],
        true
    );
    assert_eq!(
        source["dedicated_stt_no_preroll_auto_vad"]["has_preroll_buffer"],
        false
    );
}
