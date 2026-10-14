//! Real loopback Gemini video wire; no live camera, provider or browser claim.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;
const WAIT: Duration = Duration::from_secs(5);
// Small fixed fixture scripts deliberately keep original frame values inline.
#[allow(clippy::large_enum_variant)]
enum Step {
    Frame(Frame, Direction, bool),
    Pause(bool),
    Wait,
}
fn rgb(user: bool) -> Frame {
    let image = ImageRawFrame {
        image: vec![255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255],
        size: (2, 2),
        format: Some("RGB".into()),
    };
    Frame::new(if user {
        FrameData::UserImage(UserImageRawFrame {
            image,
            user_id: "camera-user".into(),
            text: Some("preserve only".into()),
            append_to_context: Some(true),
            request: None,
            run_llm: Some(false),
        })
    } else {
        FrameData::InputImage(image)
    })
}
fn down(frame: Frame) -> Step {
    Step::Frame(frame, Direction::Downstream, false)
}
fn invalid() -> Frame {
    Frame::new(FrameData::InputImage(ImageRawFrame {
        image: vec![1],
        size: (2, 2),
        format: Some("RGB".into()),
    }))
}
async fn execute(
    mut config: GeminiConfig,
    paused: bool,
    sessions: Vec<Vec<Step>>,
    cancel: bool,
) -> Vec<Vec<Value>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let count = sessions.len();
    let processor = Arc::new(
        GeminiLiveProcessor::new(GeminiClient::new(config).unwrap())
            .with_video_input_paused(paused),
    );
    let mut peer = tokio::spawn(async move {
        let mut traces = Vec::new();
        for _ in 0..count {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "setup deadline")?
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            let setup: Value = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if setup["setup"]["model"] != "models/video-fixture" {
                return Err("wrong setup".into());
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
                    .map_err(|_| "read deadline")?
                {
                    Some(Ok(Message::Text(text))) => {
                        let v = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        trace.push(v);
                    }
                    Some(Ok(Message::Ping(p))) => {
                        ws.send(Message::Pong(p)).await.map_err(|e| e.to_string())?
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        let _ = ws.flush().await;
                        break;
                    }
                    None if cancel => break,
                    Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                    ))) if cancel => break,
                    other => return Err(format!("expected close/Cancel EOF: {other:?}")),
                }
            }
            traces.push(trace);
        }
        Ok::<_, String>(traces)
    });
    let mut failures = Vec::new();
    for steps in sessions {
        let expected_errors = steps
            .iter()
            .filter(|s| matches!(s, Step::Frame(_, _, true)))
            .count() as u64;
        let created = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(2),
                start: StartFrame {
                    audio_out_sample_rate: 24_000,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await;
        let mut worker = match created {
            Ok(w) => w,
            Err(e) => {
                failures.push(e.to_string());
                break;
            }
        };
        let observed = timeout(WAIT, async {
            loop {
                if worker
                    .recv()
                    .await
                    .ok_or("closed before Start")?
                    .frame
                    .data
                    .is_start()
                {
                    break;
                }
            }
            for step in steps {
                match step {
                    Step::Pause(p) => processor.set_video_input_paused(p),
                    Step::Wait => tokio::time::sleep(Duration::from_millis(1050)).await,
                    Step::Frame(mut frame, dir, rejected) => {
                        frame.name = "video-envelope".into();
                        frame.set_pipeline_pts(4321);
                        frame.metadata.insert("camera".into(), json!("original"));
                        let tag = LocalFrameTag::new();
                        tag.mark(&mut frame).map_err(|e| e.to_string())?;
                        let expected = frame.clone();
                        worker
                            .sender()
                            .send(frame, dir)
                            .await
                            .map_err(|e| e.to_string())?;
                        loop {
                            let out = worker.recv().await.ok_or("closed before observation")?;
                            if matches!(
                                out.frame.data,
                                FrameData::Error(_) | FrameData::ErrorDetailed(_)
                            ) {
                                if !rejected {
                                    return Err(format!("unexpected error: {:?}", out.frame.data));
                                }
                                break;
                            }
                            if out.frame.id == expected.id && out.direction == dir {
                                if rejected
                                    || out.frame.data != expected.data
                                    || out.frame.metadata != expected.metadata
                                    || out.frame.pts != expected.pts
                                    || !tag.contains(&out.frame)
                                {
                                    return Err("envelope changed".into());
                                }
                                break;
                            }
                        }
                        if !worker.processor_health(0).ok_or("no health")?.is_usable() {
                            return Err("video rejection made service unusable".into());
                        }
                    }
                }
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "observation deadline".to_owned())
        .and_then(|v| v);
        let normal = observed.is_ok();
        if let Err(e) = observed {
            failures.push(e);
        }
        let admission = timeout(WAIT, async {
            if cancel || !normal {
                worker.cancel("video fixture").await
            } else {
                worker.end().await
            }
        })
        .await;
        if !matches!(admission, Ok(Ok(()))) {
            failures.push(format!("terminal admission {admission:?}"));
            let _ = timeout(WAIT, worker.cancel("terminal deadline")).await;
        }
        let joined = match timeout(WAIT, async {
            while worker.recv().await.is_some() {}
            worker.join().await
        })
        .await
        {
            Ok(r) => r,
            Err(e) => {
                failures.push(e.to_string());
                let _ = timeout(WAIT, worker.cancel("join deadline")).await;
                worker.join().await
            }
        };
        match joined {
            Ok(r) => {
                if r.total_errors != expected_errors
                    || r.state
                        != if cancel {
                            WorkerState::Cancelled
                        } else {
                            WorkerState::Ended
                        }
                {
                    failures.push(format!("report {r:?}"));
                }
            }
            Err(e) => failures.push(e.to_string()),
        }
        if !failures.is_empty() {
            break;
        }
    }
    let traces = match timeout(WAIT, &mut peer).await {
        Ok(r) => r.map_err(|e| e.to_string()).and_then(|r| r),
        Err(e) => {
            peer.abort();
            let _ = peer.await;
            Err(format!("peer aborted and joined: {e}"))
        }
    };
    assert!(failures.is_empty(), "{failures:?}; peer={traces:?}");
    traces.unwrap()
}
fn config() -> GeminiConfig {
    GeminiConfig::new("fixture-key", "video-fixture")
}
fn videos(trace: &[Value]) -> Vec<Vec<u8>> {
    trace
        .iter()
        .map(|v| {
            assert_eq!(v["realtimeInput"].as_object().unwrap().len(), 1);
            assert_eq!(v["realtimeInput"]["video"]["mimeType"], "image/jpeg");
            STANDARD
                .decode(v["realtimeInput"]["video"]["data"].as_str().unwrap())
                .unwrap()
        })
        .collect()
}
#[tokio::test]
async fn rgb_and_user_image_share_service_throttle_and_preserve_frames() {
    let t = execute(
        config(),
        false,
        vec![vec![
            down(rgb(false)),
            down(rgb(true)),
            Step::Wait,
            down(rgb(true)),
        ]],
        false,
    )
    .await;
    let v = videos(&t[0]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0], v[1]);
    assert!(v[0].starts_with(&[255, 216]));
    assert!(v[0].ends_with(&[255, 217]));
    assert!(
        v[0].windows(9)
            .any(|w| w == [255, 192, 0, 17, 8, 0, 2, 0, 2]),
        "JPEG dimensions"
    );
}
#[tokio::test]
async fn pause_ignores_invalid_pixels_and_setup_restores_initial_pause() {
    let t = execute(
        config(),
        true,
        vec![
            vec![
                down(invalid()),
                Step::Pause(false),
                down(rgb(false)),
                Step::Pause(true),
                Step::Wait,
                down(invalid()),
            ],
            vec![down(invalid()), Step::Pause(false), down(rgb(true))],
        ],
        false,
    )
    .await;
    assert_eq!(videos(&t[0]).len(), 1);
    assert_eq!(videos(&t[1]).len(), 1);
}
#[tokio::test]
async fn malformed_pixels_consume_window_but_are_recoverable() {
    let t = execute(
        config(),
        false,
        vec![vec![
            Step::Frame(invalid(), Direction::Downstream, true),
            down(rgb(false)),
            Step::Wait,
            down(rgb(false)),
        ]],
        false,
    )
    .await;
    assert_eq!(videos(&t[0]).len(), 1);
}
#[tokio::test]
async fn upstream_pixels_bypass_recognition_and_do_not_consume_window() {
    let t = execute(
        config(),
        false,
        vec![vec![
            Step::Frame(invalid(), Direction::Upstream, false),
            down(rgb(true)),
        ]],
        false,
    )
    .await;
    assert_eq!(videos(&t[0]).len(), 1);
}
#[tokio::test]
async fn exact_wire_budget_rejects_jpeg_without_network_and_keeps_health() {
    let mut c = config();
    c.limits.max_message_bytes = 512;
    c.limits.max_audio_bytes = 128;
    let t = execute(
        c,
        false,
        vec![vec![Step::Frame(rgb(false), Direction::Downstream, true)]],
        false,
    )
    .await;
    assert!(t[0].is_empty());
}
#[tokio::test]
async fn cancel_joins_session_and_reuse_resets_video_window() {
    let first = rgb(false);
    let mut second = rgb(true);
    if let FrameData::UserImage(ref mut image) = second.data {
        image.image.image = vec![0, 0, 255, 0, 0, 255, 0, 0, 255, 0, 0, 255];
    }
    let baseline = execute(
        config(),
        false,
        vec![vec![down(first.clone())], vec![down(second.clone())]],
        false,
    )
    .await;
    let expected = baseline
        .iter()
        .map(|t| videos(t).remove(0))
        .collect::<Vec<_>>();
    assert_ne!(expected[0], expected[1]);
    let traces = execute(
        config(),
        false,
        vec![vec![down(first)], vec![down(second)]],
        true,
    )
    .await;
    // Cancellation may drop pending writes; any written image must belong to that run.
    for (index, trace) in traces.iter().enumerate() {
        let images = videos(trace);
        assert!(images.len() <= 1);
        for image in images {
            assert_eq!(image, expected[index], "cross-run video replay");
        }
    }
}

#[tokio::test]
async fn normal_sequential_reuse_resets_one_second_window() {
    let t = execute(
        config(),
        false,
        vec![vec![down(rgb(false))], vec![down(rgb(true))]],
        false,
    )
    .await;
    assert_eq!(videos(&t[0]).len(), 1);
    assert_eq!(videos(&t[1]).len(), 1);
}

#[test]
fn pinned_source_throttles_before_codec_and_failed_codec_keeps_timestamp() {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/python_video_source.json")).unwrap();
    assert!(source["paused_direct"]["events"]
        .as_array()
        .unwrap()
        .is_empty());
    let rows = source["frombytes_failure_consumes_timestamp"]
        .as_array()
        .unwrap();
    assert_eq!(rows[0]["escaped_exception"], "ValueError");
    assert_eq!(rows[0]["last_sent_time"], 30.0);
    assert_eq!(rows[1]["last_sent_time"], 30.0);
    assert!(!rows[1]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event"] == "send_attempt"));
    assert!(rows[2]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event"] == "send_success"));
    let shared = source["global_throttle_across_frame_types_users_sources_and_directions"]
        .as_array()
        .unwrap();
    assert_eq!(shared[1]["input_type"], "UserImageRawFrame");
    assert_eq!(shared[1]["result"]["last_sent_time"], 100.0);
}

#[tokio::test]
async fn manual_audio_vad_does_not_require_or_synthesize_activity_for_video() {
    let mut c = config();
    c.session.activity_detection.disabled = true;
    let t = execute(c, false, vec![vec![down(rgb(false))]], false).await;
    assert_eq!(videos(&t[0]).len(), 1);
}
