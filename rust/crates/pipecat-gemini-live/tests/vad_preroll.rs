//! Real loopback WebSocket qualification of VAD control → native preroll.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;
const WAIT: Duration = Duration::from_secs(5);

type Step = (Frame, Direction, bool);
fn control(seconds: Option<f64>) -> Frame {
    Frame::new(FrameData::SpeechControlParams(SpeechControlParamsFrame {
        vad_params: seconds.map(|start_secs| VadParams {
            start_secs,
            ..Default::default()
        }),
        turn_params: None,
    }))
}
fn down(frame: Frame) -> Step {
    (frame, Direction::Downstream, false)
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
fn patterned(n: usize) -> Vec<u8> {
    (0..n).map(|n| (n % 251) as u8).collect()
}

async fn execute(
    mut config: GeminiConfig,
    fixed: Option<u32>,
    sessions: Vec<Vec<Step>>,
) -> Vec<Vec<Value>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let count = sessions.len();
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
                            ws.send(Message::Text(
                                json!({"serverContent":{"turnComplete":true}})
                                    .to_string()
                                    .into(),
                            ))
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
        let expected_errors = steps.iter().filter(|s| s.2).count() as u64;
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
            for (mut frame, direction, reject) in steps {
                let tag = LocalFrameTag::new();
                tag.mark(&mut frame).map_err(|e| e.to_string())?;
                frame.name = "preroll-control-flow".into();
                frame
                    .metadata
                    .insert("witness".into(), json!({"preserved":true}));
                frame.set_pipeline_pts(1234);
                let expected = frame.clone();
                worker
                    .sender()
                    .send(frame, direction)
                    .await
                    .map_err(|e| e.to_string())?;
                loop {
                    let out = worker.recv().await.ok_or("closed before forwarding")?;
                    if matches!(
                        out.frame.data,
                        FrameData::Error(_) | FrameData::ErrorDetailed(_)
                    ) {
                        if !reject {
                            return Err(format!("unexpected error: {:?}", out.frame.data));
                        }
                        break;
                    }
                    if out.frame.id == expected.id && out.direction == direction {
                        if reject {
                            return Err("invalid proposal was accepted".into());
                        }
                        if !tag.contains(&out.frame)
                            || out.frame.name != expected.name
                            || out.frame.metadata != expected.metadata
                            || out.frame.pts != expected.pts
                        {
                            return Err("forwarded envelope or local identity changed".into());
                        }
                        break;
                    }
                }
                if !worker
                    .processor_health(0)
                    .ok_or("missing health")?
                    .is_usable()
                    || !worker.unusable_processors().is_empty()
                {
                    return Err("proposal rejection changed service usability".into());
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
                worker.cancel("preroll observation failed").await
            }
        })
        .await;
        if !matches!(admission, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admission:?}"));
            let _ = worker.cancel("terminal deadline").await;
        }
        let joined = match timeout(WAIT, async {
            while worker.recv().await.is_some() {}
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
async fn source_default_control_growth_and_original_forwarding() {
    for (seconds, retained) in [(None, 16_000), (Some(0.5), 19_200), (Some(1.0), 35_200)] {
        let bytes = patterned(40_000);
        let traces = execute(
            config(true),
            None,
            vec![vec![
                down(control(seconds)),
                down(audio(bytes.clone(), false)),
                start(),
                stop(),
            ]],
        )
        .await;
        assert_eq!(
            pcm(&traces[0]),
            vec![bytes[bytes.len() - retained..].to_vec()]
        );
        assert_eq!(traces[0].len(), 3);
        assert_eq!(traces[0][0], json!({"realtimeInput":{"activityStart":{}}}));
        assert_eq!(traces[0][2], json!({"realtimeInput":{"activityEnd":{}}}));
    }
}
#[tokio::test]
async fn shrink_is_immediate_growth_does_not_recreate_evicted_pcm_and_active_update_only_affects_next_turn(
) {
    let bytes = patterned(40_000);
    let traces = execute(
        config(true),
        None,
        vec![vec![
            down(control(Some(0.5))),
            down(audio(bytes.clone(), true)),
            down(control(Some(0.1))),
            down(control(Some(0.7))),
            start(),
            down(control(Some(0.2))),
            down(audio(vec![1, 0, 2, 0], false)),
            stop(),
            down(audio(bytes.clone(), false)),
            start(),
            stop(),
        ]],
    )
    .await;
    assert_eq!(
        pcm(&traces[0]),
        vec![
            bytes[33_600..].to_vec(),
            vec![1, 0, 2, 0],
            bytes[30_400..].to_vec()
        ]
    );
    assert_eq!(traces[0].len(), 7);
}
#[tokio::test]
async fn fixed_and_zero_overrides_ignore_even_invalid_controls() {
    for (fixed, retained) in [(0, 0), (100, 3200)] {
        let bytes = patterned(20_000);
        let mut steps = vec![];
        for value in [f64::NAN, f64::INFINITY, -1.0, 1e308, 0.5] {
            steps.push(down(control(Some(value))));
        }
        steps.extend([down(audio(bytes.clone(), true)), start(), stop()]);
        let traces = execute(config(true), Some(fixed), vec![steps]).await;
        let expected = if retained == 0 {
            vec![]
        } else {
            vec![bytes[bytes.len() - retained..].to_vec()]
        };
        assert_eq!(pcm(&traces[0]), expected);
        assert_eq!(traces[0].len(), if retained == 0 { 2 } else { 3 });
    }
}
#[tokio::test]
async fn invalid_updates_are_atomic_nonpermanent_and_valid_later_controls_work() {
    let bytes = patterned(20_000);
    let mut steps = vec![down(control(Some(0.1))), down(audio(bytes.clone(), false))];
    for value in [f64::NAN, f64::INFINITY, -1.0, 1e308, 10.0] {
        steps.push((control(Some(value)), Direction::Downstream, true));
    }
    steps.extend([
        start(),
        stop(),
        down(control(Some(0.5))),
        down(audio(bytes.clone(), true)),
        start(),
        stop(),
    ]);
    let traces = execute(config(true), None, vec![steps]).await;
    assert_eq!(
        pcm(&traces[0]),
        vec![bytes[13_600..].to_vec(), bytes[800..].to_vec()]
    );
}
#[tokio::test]
async fn encoded_wire_limit_rejection_preserves_last_valid_window() {
    let mut c = config(true);
    c.limits.max_message_bytes = 9000;
    c.limits.max_audio_bytes = 8000;
    let bytes = patterned(8000);
    let traces = execute(
        c,
        None,
        vec![vec![
            down(control(Some(0.05))),
            down(audio(bytes.clone(), false)),
            (control(Some(0.15)), Direction::Downstream, true),
            start(),
            stop(),
        ]],
    )
    .await;
    assert_eq!(pcm(&traces[0]), vec![bytes[3200..].to_vec()]);
}
#[tokio::test]
async fn upstream_and_no_vad_controls_do_not_resize_and_fractional_capacity_keeps_whole_samples() {
    let bytes = patterned(20_000);
    let traces = execute(
        config(true),
        None,
        vec![vec![
            (control(Some(2.0)), Direction::Upstream, false),
            down(audio(bytes.clone(), false)),
            start(),
            stop(),
            down(control(Some(0.00004))),
            down(control(None)),
            down(audio(bytes.clone(), true)),
            start(),
            stop(),
        ]],
    )
    .await;
    assert_eq!(
        pcm(&traces[0]),
        vec![bytes[4000..].to_vec(), bytes[16_800..].to_vec()]
    );
}
#[tokio::test]
async fn automatic_server_vad_still_streams_all_pcm_without_activity_or_control_wire() {
    let bytes = patterned(20_000);
    let traces = execute(
        config(false),
        None,
        vec![vec![
            down(control(Some(0.1))),
            down(audio(bytes.clone(), false)),
            down(control(Some(0.5))),
            down(audio(vec![1, 0, 2, 0], true)),
        ]],
    )
    .await;
    assert_eq!(pcm(&traces[0]), vec![bytes, vec![1, 0, 2, 0]]);
    assert_eq!(traces[0].len(), 3);
    assert_eq!(
        traces[0][2],
        json!({"realtimeInput":{"audioStreamEnd":true}})
    );
}
#[tokio::test]
async fn completed_sequential_reuse_resets_automatic_capacity_and_retained_audio() {
    let bytes = patterned(40_000);
    let traces = execute(
        config(true),
        None,
        vec![
            vec![
                down(control(Some(1.0))),
                down(audio(bytes.clone(), false)),
                start(),
                stop(),
            ],
            vec![down(audio(bytes.clone(), true)), start(), stop()],
        ],
    )
    .await;
    assert_eq!(pcm(&traces[0]), vec![bytes[4800..].to_vec()]);
    assert_eq!(pcm(&traces[1]), vec![bytes[24_000..].to_vec()]);
}

#[test]
fn pinned_source_records_shared_contract_and_explicit_native_boundaries() {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/python_preroll_source.json")).unwrap();
    assert_eq!(
        source["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(source["constants"]["DEFAULT_USER_AUDIO_PREROLL_SECS"], 0.5);
    assert_eq!(
        source["constants"]["AUTOSIZED_USER_AUDIO_PREROLL_MARGIN_SECS"],
        0.1
    );
    let cases = &source["scenarios"];
    assert_eq!(cases["control_grow"][0]["preroll_secs"], 0.9);
    assert_eq!(
        cases["control_shrink_no_new_audio"][1]["buffer"]["bytes"],
        16000
    );
    assert_eq!(
        cases["control_shrink_next_idle_audio"][2]["buffer"]["bytes"],
        6400
    );
    assert_eq!(
        cases["fractional_duration_can_split_pcm16_sample"][0]["buffer"]["bytes"],
        3201
    );
}
