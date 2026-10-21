mod common;
use common::*;
use pipecat_aws_sagemaker_bidi_tts::{SageMakerBidiTtsApp, Terminal};
use pipecat_core::FrameData;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn two_real_sessions_stream_before_completion_and_preserve_contexts() {
    let output = Arc::new(Output::default());
    let observed = output.clone();
    let (origin, peer) = peer_sessions(2, move |index, request, mut respond| {
        let observed = observed.clone();
        async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, if index == 0 { "first" } else { "second" }).await;
            // Headers intentionally withheld until all text startup arrives.
            let mut stream = respond.send_response(response(), false).unwrap();
            let pcm = vec![index as u8 + 1; 24000];
            send(&mut stream, &pcm_event(&pcm[..23999]), false, 4096).await;
            sleep(Duration::from_millis(20)).await;
            assert_eq!(observed.frames.lock().unwrap().len(), index);
            send(&mut stream, &pcm_event(&pcm[23999..]), false, 7).await;
            timeout(Duration::from_secs(2), observed.seen.notified())
                .await
                .unwrap();
            assert_eq!(observed.frames.lock().unwrap().len(), index + 1);
            complete(&mut upload, &mut stream).await;
        }
    })
    .await;
    let app = SageMakerBidiTtsApp::new(
        config(&origin),
        vec![" first ".into(), "second".into()],
        output.clone(),
    )
    .unwrap()
    .start()
    .unwrap();
    let c = app.completion();
    let _ = timeout(Duration::from_nanos(1), c.wait()).await;
    let (a, b) = tokio::join!(c.wait(), c.wait());
    assert_joined(&a);
    assert!(a.is_success(), "{a:?}");
    assert_eq!(a.terminal, b.terminal);
    assert_eq!(a.utterances.len(), 2);
    assert_ne!(a.utterances[0].context_id, a.utterances[1].context_id);
    {
        let frames = output.frames.lock().unwrap();
        for (i, f) in frames.iter().enumerate() {
            let FrameData::TtsAudio(a) = f else { panic!() };
            assert_eq!(
                a.context_id.as_deref(),
                Some(if i == 0 {
                    "sagemaker-bidi-1"
                } else {
                    "sagemaker-bidi-2"
                })
            );
            assert_eq!(a.audio, vec![i as u8 + 1; 24000]);
        }
    }
    timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn raw_pcm_short_tail_and_empty_completion_are_distinct() {
    let (origin, peer) = peer_sessions(2, |index, request, mut respond| async move {
        let mut upload = Upload::new(request);
        startup(&mut upload, "hello").await;
        let mut stream = respond.send_response(response(), false).unwrap();
        if index == 0 {
            let raw = frame(
                &[
                    (":message-type", "event"),
                    (":event-type", "PayloadPart"),
                    (":content-type", "application/octet-stream"),
                    ("DataType", "BINARY"),
                ],
                &[255, 1, 254, 2],
            );
            send(&mut stream, &raw, false, 7).await;
        }
        complete(&mut upload, &mut stream).await;
    })
    .await;
    let output = Arc::new(Output::default());
    let app = SageMakerBidiTtsApp::new(config(&origin), vec!["hello".into(); 2], output.clone())
        .unwrap()
        .start()
        .unwrap();
    let report = app.finish().await;
    assert_joined(&report);
    assert!(report.is_success(), "{report:?}");
    assert_eq!(recorded(&output), [255, 1, 254, 2]);
    assert_eq!(report.utterances[1].progress.pcm_bytes_delivered, 0);
    peer.await.unwrap();
}
#[tokio::test]
async fn interrupt_jitter_tail_then_next_utterance_no_stale_pcm_or_replay() {
    let ready = Arc::new(tokio::sync::Notify::new());
    let notification = ready.clone();
    let (origin, peer) = peer_sessions(2, move |index, request, mut respond| {
        let ready = notification.clone();
        async move {
            let mut upload = Upload::new(request);
            startup(
                &mut upload,
                if index == 0 { "interrupt me" } else { "fresh" },
            )
            .await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if index == 0 {
                send(&mut stream, &pcm_event(&[9, 9]), false, 7).await;
                ready.notify_one();
                assert_eq!(
                    upload.next().await.unwrap(),
                    json!({"type":"input_text.done"})
                );
                // A late PCM response is merely observed, never accepted as proof of remote stop.
                send(&mut stream, &pcm_event(&vec![9; 24000]), false, 4096).await;
                assert_eq!(upload.next().await.unwrap(), json!({"type":"session.end"}));
                upload.finish().await;
                send(&mut stream, &[], true, 1).await;
            } else {
                send(&mut stream, &pcm_event(&[1, 2, 3, 4]), false, 7).await;
                complete(&mut upload, &mut stream).await;
            }
        }
    })
    .await;
    let output = Arc::new(Output::default());
    let app = SageMakerBidiTtsApp::new(
        config(&origin),
        vec!["interrupt me".into(), "fresh".into()],
        output.clone(),
    )
    .unwrap()
    .start()
    .unwrap();
    timeout(Duration::from_secs(2), ready.notified())
        .await
        .unwrap();
    app.interrupt();
    let report = timeout(Duration::from_secs(8), app.finish()).await.unwrap();
    assert_joined(&report);
    assert!(report.is_success(), "{report:?}");
    assert_eq!(report.terminal, Terminal::CompletedWithInterruption);
    assert!(report.utterances[0].progress.interrupted);
    assert!(report.utterances[0].progress.interruption_response_observed);
    assert_eq!(recorded(&output), [1, 2, 3, 4]);
    assert_eq!(
        output
            .interruptions
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    peer.await.unwrap();
}
#[tokio::test]
async fn interruption_before_first_audio_without_remote_completion_recovers() {
    let ready = Arc::new(tokio::sync::Notify::new());
    let notify = ready.clone();
    let (origin, peer) = peer_sessions(2, move |index, request, mut respond| {
        let ready = notify.clone();
        async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, "hello").await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if index == 0 {
                ready.notify_one();
                assert_eq!(upload.next().await.unwrap()["type"], "input_text.done");
                assert_eq!(upload.next().await.unwrap()["type"], "session.end");
                upload.finish().await;
                send(&mut stream, &[], true, 1).await;
            } else {
                send(&mut stream, &pcm_event(&[4, 3]), false, 7).await;
                complete(&mut upload, &mut stream).await;
            }
        }
    })
    .await;
    let output = Arc::new(Output::default());
    let app = SageMakerBidiTtsApp::new(config(&origin), vec!["hello".into(); 2], output.clone())
        .unwrap()
        .start()
        .unwrap();
    ready.notified().await;
    assert_eq!(output.starts.load(std::sync::atomic::Ordering::Acquire), 1);
    assert!(recorded(&output).is_empty());
    app.interrupt();
    let report = app.finish().await;
    assert_joined(&report);
    assert!(report.is_success(), "{report:?}");
    assert!(!report.utterances[0].progress.interruption_response_observed);
    assert_eq!(recorded(&output), [4, 3]);
    peer.await.unwrap();
}
#[tokio::test]
async fn malformed_provider_odd_pcm_and_eof_fail_sanitized() {
    for mode in 0..5 {
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, "hello").await;
            let mut stream = respond.send_response(response(), false).unwrap();
            let wire = match mode {
                0 => event(json!({"type":"error","message":"SECRET"})),
                1 => event(json!(42)),
                2 => pcm_event(&[1]),
                3 => {
                    let mut x = pcm_event(&[1, 2]);
                    x[12] ^= 1;
                    x
                }
                _ => vec![],
            };
            if !wire.is_empty() {
                send(&mut stream, &wire, false, 4096).await;
            }
            if mode == 2 {
                send(
                    &mut stream,
                    &event(json!({"type":"conversation.item.speech.completed"})),
                    false,
                    4096,
                )
                .await;
            }
            if mode == 4 {
                send(&mut stream, &[], true, 1).await;
            } else {
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        let app = SageMakerBidiTtsApp::new(
            config(&origin),
            vec!["hello".into()],
            Arc::new(Output::default()),
        )
        .unwrap()
        .start()
        .unwrap();
        let r = app.finish().await;
        assert!(!r.is_success(), "mode {mode}: {r:?}");
        assert_joined(&r);
        assert!(!format!("{r:?}").contains("SECRET"));
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn cancel_stop_and_drop_join_active_read_without_detached_owners() {
    for mode in 0..3 {
        let ready = Arc::new(tokio::sync::Notify::new());
        let notify = ready.clone();
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, "hello").await;
            let _stream = respond.send_response(response(), false).unwrap();
            notify.notify_one();
            sleep(Duration::from_millis(300)).await;
        })
        .await;
        let app = SageMakerBidiTtsApp::new(
            config(&origin),
            vec!["hello".into(), "never replay".into()],
            Arc::new(Output::default()),
        )
        .unwrap()
        .start()
        .unwrap();
        let c = app.completion();
        ready.notified().await;
        match mode {
            0 => {
                app.cancel();
                drop(app);
            }
            1 => {
                app.stop();
                drop(app);
            }
            _ => drop(app),
        };
        let report = timeout(Duration::from_secs(5), c.wait()).await.unwrap();
        assert_eq!(report.terminal, Terminal::Cancelled);
        assert_joined(&report);
        assert_eq!(report.utterances.len(), 1);
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn output_panic_and_cleanup_error_are_not_success() {
    for cleanup in [false, true] {
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, "hello").await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if cleanup {
                send(&mut stream, &pcm_event(&[1, 2]), false, 4096).await;
                complete(&mut upload, &mut stream).await;
            } else {
                send(&mut stream, &pcm_event(&vec![1; 24000]), false, 4096).await;
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        let output = Arc::new(Output {
            panic: !cleanup,
            cleanup_error: cleanup,
            ..Default::default()
        });
        let app = SageMakerBidiTtsApp::new(config(&origin), vec!["hello".into()], output)
            .unwrap()
            .start()
            .unwrap();
        let r = app.finish().await;
        assert!(!r.is_success(), "{r:?}");
        assert_joined(&r);
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn preflight_and_unicode_skips_do_not_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let c = config(&origin);
    assert!(SageMakerBidiTtsApp::new(
        c.clone(),
        vec!["ok".into(), "x".repeat(c.limits().max_text_bytes + 1)],
        Arc::new(Output::default())
    )
    .is_err());
    let app = SageMakerBidiTtsApp::new(
        c,
        vec!["\u{1c} \u{3000}".into(), "!? 🎉".into()],
        Arc::new(Output::default()),
    )
    .unwrap()
    .start()
    .unwrap();
    let r = app.finish().await;
    assert!(r.is_success(), "{r:?}");
    assert_eq!(r.terminal, Terminal::CompletedWithoutAudio);
    assert!(r.utterances.iter().all(|u| u.progress.skipped));
    assert!(timeout(Duration::from_millis(50), listener.accept())
        .await
        .is_err());
}

#[derive(Default)]
struct BlockFirstAudio {
    blocked: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    pcm: std::sync::Mutex<Vec<u8>>,
}
#[pipecat_core::async_trait]
impl pipecat_core::FrameProcessor for BlockFirstAudio {
    fn name(&self) -> &str {
        "BlockFirstAudio"
    }
    async fn process(
        &self,
        frame: pipecat_core::Frame,
        direction: pipecat_core::Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if let FrameData::TtsAudio(audio) = &frame.data {
            if !self.blocked.swap(true, std::sync::atomic::Ordering::AcqRel) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.pcm.lock().unwrap().extend_from_slice(&audio.audio);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn interruption_releases_blocked_output_before_clean_second_session() {
    let (origin, peer) = peer_sessions(2, |index, request, mut respond| async move {
        let mut upload = Upload::new(request);
        startup(&mut upload, "hello").await;
        let mut stream = respond.send_response(response(), false).unwrap();
        if index == 0 {
            send(&mut stream, &pcm_event(&vec![8; 24000]), false, 4096).await;
            assert_eq!(upload.next().await.unwrap()["type"], "input_text.done");
            send(
                &mut stream,
                &event(json!({"type":"conversation.item.speech.completed"})),
                false,
                4096,
            )
            .await;
            assert_eq!(upload.next().await.unwrap()["type"], "session.end");
            upload.finish().await;
            send(&mut stream, &[], true, 1).await;
        } else {
            send(&mut stream, &pcm_event(&[5, 6]), false, 4096).await;
            complete(&mut upload, &mut stream).await;
        }
    })
    .await;
    let output = Arc::new(BlockFirstAudio::default());
    let app = SageMakerBidiTtsApp::new(config(&origin), vec!["hello".into(); 2], output.clone())
        .unwrap()
        .start()
        .unwrap();
    timeout(Duration::from_secs(2), output.entered.notified())
        .await
        .unwrap();
    app.interrupt();
    let r = timeout(Duration::from_secs(6), app.finish()).await.unwrap();
    assert_joined(&r);
    assert!(r.is_success(), "{r:?}");
    assert_eq!(*output.pcm.lock().unwrap(), [5, 6]);
    peer.await.unwrap();
}
#[tokio::test]
async fn late_provider_error_after_interruption_is_failure_not_remote_stop() {
    let ready = Arc::new(tokio::sync::Notify::new());
    let notify = ready.clone();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut upload = Upload::new(request);
        startup(&mut upload, "hello").await;
        let mut stream = respond.send_response(response(), false).unwrap();
        notify.notify_one();
        assert_eq!(upload.next().await.unwrap()["type"], "input_text.done");
        send(
            &mut stream,
            &event(json!({"type":"input_text.committed"})),
            false,
            4096,
        )
        .await;
        assert_eq!(upload.next().await.unwrap()["type"], "session.end");
        upload.finish().await;
        send(
            &mut stream,
            &event(json!({"type":"error","message":"SECRET"})),
            true,
            4096,
        )
        .await;
    })
    .await;
    let app = SageMakerBidiTtsApp::new(
        config(&origin),
        vec!["hello".into()],
        Arc::new(Output::default()),
    )
    .unwrap()
    .start()
    .unwrap();
    ready.notified().await;
    app.interrupt();
    let r = app.finish().await;
    assert_joined(&r);
    assert!(!r.is_success(), "{r:?}");
    assert_eq!(
        r.primary,
        Some(pipecat_aws_sagemaker_bidi_tts::Error::Provider)
    );
    peer.await.unwrap();
}
#[tokio::test]
async fn silent_peer_hits_absolute_budget_and_joins() {
    let (origin, peer) = peer(|request, mut respond| async move {
        let mut upload = Upload::new(request);
        startup(&mut upload, "hello").await;
        let _stream = respond.send_response(response(), false).unwrap();
        sleep(Duration::from_millis(600)).await;
    })
    .await;
    let mut limits = config(&origin).limits().clone();
    limits.app_timeout = Duration::from_millis(150);
    let c = config(&origin).with_limits(limits).unwrap();
    let app = SageMakerBidiTtsApp::new(c, vec!["hello".into()], Arc::new(Output::default()))
        .unwrap()
        .start()
        .unwrap();
    let r = timeout(Duration::from_secs(3), app.finish()).await.unwrap();
    assert_joined(&r);
    assert!(!r.is_success());
    peer.await.unwrap();
}
