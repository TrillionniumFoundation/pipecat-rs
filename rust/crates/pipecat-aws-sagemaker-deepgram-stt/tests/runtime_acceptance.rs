mod common;
use common::*;
use pipecat_aws_sagemaker_deepgram_stt::{SageMakerSttApp, Terminal, Timeline, TimelineMarker};
use pipecat_core::FrameData;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time::{timeout, Instant};

#[tokio::test]
async fn pre_head_raw_audio_and_fragmented_response_full_joins() {
    let pcm: Vec<u8> = (0..12800).map(|i| (i % 251) as u8).collect();
    let expected = pcm.clone();
    let output = Arc::new(Output::default());
    let observed = output.clone();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut upload = Upload::new(request);
        let first = upload.next().await.unwrap();
        assert!(first.get("binary").is_some(), "no startup config");
        let mut stream = respond.send_response(response(), false).unwrap();
        send(
            &mut stream,
            &event(result(" during upload ", false, true)),
            false,
            7,
        )
        .await;
        timeout(Duration::from_secs(3), observed.seen.notified())
            .await
            .unwrap();
        upload.finish().await;
        assert_pcm(&upload.events, &expected);
        send(
            &mut stream,
            &event(result(" complete ", true, true)),
            true,
            7,
        )
        .await;
    })
    .await;
    let app = SageMakerSttApp::new(config(&origin), pcm, output.clone())
        .unwrap()
        .start()
        .unwrap();
    let c = app.completion();
    let _ = timeout(Duration::from_nanos(1), c.wait()).await;
    let (a, b) = timeout(Duration::from_secs(15), async {
        tokio::join!(c.wait(), c.wait())
    })
    .await
    .unwrap();
    assert_joined(&a);
    assert!(a.is_success(), "{a:?}");
    assert_eq!(a.terminal, b.terminal);
    assert_eq!(a.progress.pcm_bytes_uploaded, 12800);
    assert_eq!(a.progress.chunks_written, 4);
    assert_eq!(a.progress.final_frames, 1);
    assert_eq!(a.progress.interim_frames, 1);
    {
        let frames = output.frames.lock().unwrap();
        let FrameData::TranscriptionDetailed(f) = &frames[1] else {
            panic!()
        };
        assert!(!f.finalized, "unsolicited confirmation");
        assert_eq!(f.language.as_deref(), Some("en-US"));
    }
    peer.await.unwrap();
}

/// Actual production period, no paused clock or test-only timing override.
#[tokio::test]
async fn real_twelve_second_pause_sends_two_keepalives_before_second_audio() {
    let (origin, peer) = peer(|request, mut respond| async move {
        let mut upload = Upload::new(request);
        assert!(upload.next().await.unwrap().get("binary").is_some());
        let accepted_lower_bound = Instant::now();
        let mut stream = respond.send_response(response(), false).unwrap();
        let mut times = vec![];
        loop {
            let value = upload.next().await.unwrap();
            if value.get("binary").is_some() {
                break;
            }
            assert_eq!(value["type"], "KeepAlive");
            times.push(accepted_lower_bound.elapsed());
        }
        assert!(times.len() >= 2, "{times:?}");
        assert!(times[0] >= Duration::from_secs(5));
        assert!(times[1] >= Duration::from_secs(10));
        assert_eq!(upload.next().await.unwrap()["type"], "Finalize");
        send(
            &mut stream,
            &event(result("confirmed", true, true)),
            false,
            4096,
        )
        .await;
        upload.finish().await;
        assert_pcm(&upload.events, &vec![7; 6400]);
        assert_eq!(upload.events.last().unwrap()["type"], "CloseStream");
        send(&mut stream, &[], true, 1).await;
    })
    .await;
    let output = Arc::new(Output::default());
    let app = SageMakerSttApp::new(config(&origin), vec![7; 6400], output.clone())
        .unwrap()
        .with_timeline(Timeline(vec![
            TimelineMarker {
                after_chunk: 1,
                pause_ms: 12000,
                vad_stopped: false,
            },
            TimelineMarker {
                after_chunk: 2,
                pause_ms: 0,
                vad_stopped: true,
            },
        ]))
        .unwrap()
        .start()
        .unwrap();
    let report = timeout(Duration::from_secs(40), app.finish())
        .await
        .unwrap();
    assert_joined(&report);
    assert!(report.is_success(), "{report:?}");
    assert!(report.progress.keepalive_written >= 2);
    assert_eq!(
        report.progress.keepalive_written,
        report.progress.keepalive_attempts
    );
    assert_eq!(report.progress.finalize_written, 1);
    {
        let frames = output.frames.lock().unwrap();
        let FrameData::TranscriptionDetailed(f) = &frames[0] else {
            panic!()
        };
        assert!(f.finalized);
    }
    peer.await.unwrap();
}

#[tokio::test]
async fn paced_audio_does_not_starve_periodic_keepalive_and_end_fences_timer() {
    let (origin, peer) = peer(|request, mut respond| async move {
        let mut upload = Upload::new(request);
        assert!(upload.next().await.unwrap().get("binary").is_some());
        let mut stream = respond.send_response(response(), false).unwrap();
        upload.finish().await;
        assert_pcm(&upload.events, &vec![5; 3200 * 8]);
        let keep = upload
            .events
            .iter()
            .position(|v| v["type"] == "KeepAlive")
            .expect("periodic control amid continuing audio");
        assert!(
            upload.events[..keep]
                .iter()
                .filter(|v| v.get("binary").is_some())
                .count()
                >= 4
        );
        assert!(upload.events[keep + 1..]
            .iter()
            .any(|v| v.get("binary").is_some()));
        send(&mut stream, &[], true, 1).await;
    })
    .await;
    let timeline = Timeline(
        (1..8)
            .map(|after_chunk| TimelineMarker {
                after_chunk,
                pause_ms: 900,
                vad_stopped: false,
            })
            .collect(),
    );
    let app = SageMakerSttApp::new(
        config(&origin),
        vec![5; 3200 * 8],
        Arc::new(Output::default()),
    )
    .unwrap()
    .with_timeline(timeline)
    .unwrap()
    .start()
    .unwrap();
    let r = timeout(Duration::from_secs(30), app.finish())
        .await
        .unwrap();
    assert_joined(&r);
    assert!(r.is_success(), "{r:?}");
    assert!(r.progress.keepalive_written >= 1);
    peer.await.unwrap();
}

#[tokio::test]
async fn cancel_stop_drop_interrupt_pause_and_preserve_completion_owner() {
    for mode in 0..3 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            assert!(upload.next().await.unwrap().get("binary").is_some());
            let mut stream = respond.send_response(response(), false).unwrap();
            tx.send(()).unwrap();
            std::future::poll_fn(|cx| stream.poll_reset(cx))
                .await
                .unwrap();
        })
        .await;
        let app = SageMakerSttApp::new(config(&origin), vec![0; 6400], Arc::new(Output::default()))
            .unwrap()
            .with_timeline(Timeline(vec![TimelineMarker {
                after_chunk: 1,
                pause_ms: 12000,
                vad_stopped: true,
            }]))
            .unwrap()
            .start()
            .unwrap();
        let c = app.completion();
        rx.await.unwrap();
        let began = Instant::now();
        match mode {
            0 => app.cancel(),
            1 => app.stop(),
            _ => drop(app),
        };
        let (a, b) = timeout(Duration::from_secs(5), async {
            tokio::join!(c.wait(), c.wait())
        })
        .await
        .unwrap();
        assert!(began.elapsed() < Duration::from_secs(5));
        assert_eq!(a.terminal, Terminal::Cancelled);
        assert_eq!(a.terminal, b.terminal);
        assert_joined(&a);
        assert_eq!(a.progress.finalize_written, 0);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn silence_and_partial_provider_failure_have_distinct_results() {
    for partial in [false, true] {
        let output = Arc::new(Output::default());
        let observed = output.clone();
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            upload.finish().await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if partial {
                send(
                    &mut stream,
                    &event(result("prefix", true, false)),
                    false,
                    4096,
                )
                .await;
                timeout(Duration::from_secs(2), observed.seen.notified())
                    .await
                    .unwrap();
                send(
                    &mut stream,
                    &event(json!({"type":"Error","message":SECRET})),
                    true,
                    4096,
                )
                .await;
            } else {
                let mut bytes = event(json!({"type":"Metadata"}));
                bytes.extend(event(result(" \u{1c}\u{3000}", true, true)));
                send(&mut stream, &bytes, true, 7).await;
            }
        })
        .await;
        let app = SageMakerSttApp::new(config(&origin), vec![0; 32], output)
            .unwrap()
            .start()
            .unwrap();
        let r = timeout(Duration::from_secs(15), app.finish())
            .await
            .unwrap();
        assert_joined(&r);
        assert_eq!(
            r.terminal,
            if partial {
                Terminal::PartialFailure
            } else {
                Terminal::CompletedWithoutTranscript
            }
        );
        for secret in [SECRET, TOKEN, ACCESS] {
            assert!(!format!("{r:?}").contains(secret));
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn output_failure_is_bounded_and_does_not_manufacture_success() {
    for mode in 0..3 {
        let output = Arc::new(Output {
            block: mode == 0,
            panic: mode == 1,
            cleanup_error: mode == 2,
            ..Default::default()
        });
        let (origin, peer) = peer(|request, mut respond| async move {
            let mut upload = Upload::new(request);
            upload.finish().await;
            let mut stream = respond.send_response(response(), false).unwrap();
            send(
                &mut stream,
                &event(result("result", true, false)),
                true,
                4096,
            )
            .await;
        })
        .await;
        let app = SageMakerSttApp::new(config(&origin), vec![0; 32], output)
            .unwrap()
            .start()
            .unwrap();
        let r = timeout(Duration::from_secs(15), app.finish())
            .await
            .unwrap();
        assert_joined(&r);
        assert!(!r.is_success());
        if mode == 2 {
            assert!(r.output_cleanup_failed);
        }
        peer.await.unwrap();
    }
}

#[test]
fn invalid_pcm_and_timeline_fail_before_pipeline_or_socket() {
    let config = config("http://127.0.0.1:9/");
    let output = Arc::new(Output::default());
    for pcm in [vec![], vec![1], vec![0; 8 * 1024 * 1024 + 2]] {
        assert!(SageMakerSttApp::new(config.clone(), pcm, output.clone()).is_err());
    }
    assert!(SageMakerSttApp::new(config, vec![0; 32], output)
        .unwrap()
        .with_timeline(Timeline(vec![TimelineMarker {
            after_chunk: 0,
            pause_ms: 1,
            vad_stopped: false
        }]))
        .is_err());
}
