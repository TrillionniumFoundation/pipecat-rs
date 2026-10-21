mod common;
use common::*;
use pipecat_aws_sagemaker_stt::{SageMakerSttApp, Terminal, TranscriptionSettings};
use pipecat_core::FrameData;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

#[tokio::test]
async fn finite_app_withholds_head_until_all_upload_and_checks_exact_pcm_and_wire() {
    let pcm: Vec<u8> = (0..12800).map(|i| (i % 251) as u8).collect();
    let expected = pcm.clone();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut upload = Upload::new(request);
        upload.finish().await;
        assert_pcm(&upload.events, &expected);
        assert_eq!(
            upload.events[0]["session"]["input_audio_transcription"]["model"],
            TranscriptionSettings::default().model
        );
        let mut stream = respond.send_response(response(), false).unwrap();
        let mut both = event(
            json!({"type":"conversation.item.input_audio_transcription.delta","delta":" hello"}),
        );
        both.extend(event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":" hello ","confidence":0.8})));
        // Eager one-byte DATA floods can exhaust h2's independent framing-
        // overhead budget before the EventStream consumer runs. Keep this
        // integration burst below 128 small DATA frames; bounded exhaustive
        // bytewise segmentation remains covered by the transport unit test.
        assert!(both.len().div_ceil(7) < 128);
        send(&mut stream, &both, true, 7).await;
    })
    .await;
    let output = Arc::new(Output::default());
    let app = SageMakerSttApp::new(config(&origin), pcm, output.clone())
        .unwrap()
        .start()
        .unwrap();
    let report = timeout(Duration::from_secs(8), app.finish()).await.unwrap();
    assert_joined(&report);
    assert!(report.is_success(), "{report:?}");
    assert_eq!(report.progress.pcm_bytes_uploaded, 12800);
    assert_eq!(report.progress.chunks_admitted, 4);
    assert_eq!(report.progress.final_frames, 1);
    assert_eq!(report.progress.interim_frames, 1);
    {
        let frames = output.frames.lock().unwrap();
        let FrameData::TranscriptionDetailed(f) = &frames[1] else {
            panic!()
        };
        assert_eq!(f.text.text, " hello ");
        assert!(f.finalized);
        assert_eq!(f.language.as_deref(), Some("en-US"));
        assert_eq!(f.result.as_ref().unwrap()["confidence"], 0.8);
    }
    timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn public_interim_arrives_before_peer_eof_and_while_upload_open() {
    let output = Arc::new(Output::default());
    let observed = output.clone();
    let pcm = vec![7; 32000];
    let expected = pcm.clone();
    let(origin,peer)=peer(move|request,mut respond|async move{
        let mut upload=Upload::new(request);assert_eq!(upload.next().await.unwrap()["type"],"transcription_session.update");assert_eq!(upload.next().await.unwrap()["type"],"input_audio_buffer.append");
        let mut stream=respond.send_response(response(),false).unwrap();send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.delta","delta":"during upload"})),false,7).await;
        timeout(Duration::from_secs(2),observed.seen.notified()).await.unwrap();assert_eq!(observed.frames.lock().unwrap().len(),1);
        upload.finish().await;assert_pcm(&upload.events,&expected);send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"finished"})),true,4096).await;
    }).await;
    let app = SageMakerSttApp::new(config(&origin), pcm, output)
        .unwrap()
        .start()
        .unwrap();
    let c = app.completion();
    let cancelled_wait = timeout(Duration::from_nanos(1), c.wait()).await;
    drop(cancelled_wait);
    let (a, b) = tokio::join!(c.wait(), c.wait());
    assert!(a.is_success(), "{a:?}");
    assert_eq!(a.terminal, b.terminal);
    assert_joined(&a);
    timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn silence_invalid_json_and_python_whitespace_finals_are_not_transcripts() {
    let(origin,peer)=peer(|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;let mut stream=respond.send_response(response(),false).unwrap();
        let mut bytes=vec![];for payload in [b"".as_slice(),b"bad-json",b"\xff"]{bytes.extend(frame(&[(":message-type","event"),(":event-type","PayloadPart"),(":content-type","application/octet-stream")],payload));}
        bytes.extend(event(json!({"type":"input_audio_buffer.committed"})));bytes.extend(event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"\u{1c}\u{1d}\u{1e}\u{1f}\u{3000}"})));send(&mut stream,&bytes,true,13).await;
    }).await;
    let app = SageMakerSttApp::new(config(&origin), vec![0; 3200], Arc::new(Output::default()))
        .unwrap()
        .start()
        .unwrap();
    let report = app.finish().await;
    assert_eq!(
        report.terminal,
        Terminal::CompletedWithoutTranscript,
        "{report:?}"
    );
    assert_eq!(report.progress.ignored_events, 5);
    assert_joined(&report);
    peer.await.unwrap();
}
#[tokio::test]
async fn wire_and_provider_failures_never_become_success_or_leak_diagnostics() {
    for case in 0..11 {
        let(origin,peer)=peer(move|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;let mut stream=respond.send_response(response(),false).unwrap();
            let raw=match case{
                0=>event(json!({"type":"error","message":SECRET})),
                1=>event(json!({"type":"conversation.item.input_audio_transcription.failed","message":TOKEN})),
                2=>frame(&[(":message-type","exception"),(":exception-type","ModelStreamError")],SECRET.as_bytes()),
                3=>frame(&[(":message-type","exception"),(":exception-type","InternalStreamFailure")],TOKEN.as_bytes()),
                4=>frame(&[(":message-type","exception"),(":exception-type","UnmodeledAttackerError")],SECRET.as_bytes()),
                5=>frame(&[(":message-type","event"),(":event-type","chunk"),(":content-type","application/json")],b"{}"),
                6=>{let mut raw=event(json!({"type":"unknown"}));let n=raw.len();raw[n-1]^=1;raw},
                7=>frame(&[(":message-type","event"),(":message-type","event"),(":event-type","PayloadPart"),(":content-type","application/octet-stream")],b"{}"),
                8=>frame(&[(":message-type","event"),(":event-type","PayloadPart"),(":content-type","application/octet-stream"),("DataType","utf8")],b"{}"),
                9=>event(json!({"type":"unknown","nested":vec!["x";70000]})),
                _=>invalid_datatype_event(),
            };if case==9{let _=stream.send_data(bytes::Bytes::from(raw),true);}else{send(&mut stream,&raw,true,16384).await;}
        }).await;
        let app = SageMakerSttApp::new(config(&origin), vec![0; 32], Arc::new(Output::default()))
            .unwrap()
            .start()
            .unwrap();
        let report = timeout(Duration::from_secs(8), app.finish()).await.unwrap();
        assert!(!report.is_success(), "case {case}: {report:?}");
        assert_joined(&report);
        let diagnostic = format!("{:?}", report.primary);
        for secret in [SECRET, TOKEN, ACCESS] {
            assert!(!diagnostic.contains(secret));
        }
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn partial_transcript_then_provider_failure_preserves_prior_output() {
    let output = Arc::new(Output::default());
    let seen = output.clone();
    let(origin,peer)=peer(move|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;let mut stream=respond.send_response(response(),false).unwrap();send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"prefix"})),false,11).await;timeout(Duration::from_secs(2),seen.seen.notified()).await.unwrap();send(&mut stream,&event(json!({"type":"error","message":SECRET})),true,999).await;}).await;
    let app = SageMakerSttApp::new(config(&origin), vec![0; 32], output.clone())
        .unwrap()
        .start()
        .unwrap();
    let r = app.finish().await;
    assert_eq!(r.terminal, Terminal::PartialFailure, "{r:?}");
    assert_joined(&r);
    assert_eq!(output.frames.lock().unwrap().len(), 1);
    peer.await.unwrap();
}
#[tokio::test]
async fn output_block_panic_and_cleanup_failure_are_bounded_and_joined() {
    for mode in 0..3 {
        let output = Arc::new(Output {
            block: mode == 0,
            panic: mode == 1,
            cleanup_error: mode == 2,
            ..Default::default()
        });
        let(origin,peer)=peer(|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;let mut stream=respond.send_response(response(),false).unwrap();send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"result"})),true,999).await;}).await;
        let app = SageMakerSttApp::new(config(&origin), vec![0; 32], output)
            .unwrap()
            .start()
            .unwrap();
        let r = timeout(Duration::from_secs(8), app.finish()).await.unwrap();
        assert!(!r.is_success(), "{r:?}");
        assert_joined(&r);
        if mode == 2 {
            assert!(r.output_cleanup_failed);
        }
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn cancel_stop_and_owner_drop_interrupt_pending_response_and_retain_waiters() {
    for mode in 0..3 {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (origin, peer) = peer(move |request, _respond| async move {
            let mut body = request.into_body();
            ready_tx.send(()).unwrap();
            while let Some(result) = body.data().await {
                match result {
                    Ok(b) => {
                        body.flow_control().release_capacity(b.len()).unwrap();
                    }
                    Err(_) => break,
                }
            }
        })
        .await;
        let app =
            SageMakerSttApp::new(config(&origin), vec![0; 32000], Arc::new(Output::default()))
                .unwrap()
                .start()
                .unwrap();
        let c = app.completion();
        ready_rx.await.unwrap();
        match mode {
            0 => app.cancel(),
            1 => app.stop(),
            _ => drop(app),
        };
        let (a, b) = tokio::join!(c.wait(), c.wait());
        assert_eq!(a.terminal, Terminal::Cancelled, "{a:?}");
        assert_eq!(a.terminal, b.terminal);
        assert_joined(&a);
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
#[test]
fn complete_pcm_and_settings_preflight_reject_without_starting_output() {
    let c = config("http://127.0.0.1:9/");
    let output = Arc::new(Output::default());
    for pcm in [vec![], vec![1], vec![0; 8 * 1024 * 1024 + 2]] {
        assert!(SageMakerSttApp::new(c.clone(), pcm, output.clone()).is_err());
    }
    for raw in [
        br#"{"unknown":1}"#.as_slice(),
        br#"{"sample_rate_hz":0}"#,
        br#"{"model":null}"#,
        br#"{"language":"en","language":"fr"}"#,
    ] {
        assert!(TranscriptionSettings::from_json(raw).is_err());
    }
    for endpoint in ["", "bad/path", "test?x", "-bad", "bad%2Fpath"] {
        assert!(pipecat_aws_sagemaker_stt::SageMakerSttConfig::new(
            "us-east-1",
            endpoint,
            pipecat_aws_common::AwsCredentials::new(ACCESS, SECRET, None).unwrap()
        )
        .is_err());
    }
    assert!(output.frames.lock().unwrap().is_empty());
}

#[tokio::test]
async fn two_isolated_applications_do_not_share_signers_pcm_or_outputs() {
    let mut owners = vec![];
    for index in 0..2u8 {
        let(origin,peer)=peer(move|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;assert_pcm(&u.events,&vec![index;6400]);let mut stream=respond.send_response(response(),false).unwrap();send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":format!("session-{index}")})),true,3).await;}).await;
        let output = Arc::new(Output::default());
        let app = SageMakerSttApp::new(config(&origin), vec![index; 6400], output.clone())
            .unwrap()
            .start()
            .unwrap();
        owners.push((app, output, peer));
    }
    for (index, (app, output, peer)) in owners.into_iter().enumerate() {
        let r = app.finish().await;
        assert!(r.is_success(), "{r:?}");
        assert_joined(&r);
        {
            let frames = output.frames.lock().unwrap();
            let FrameData::TranscriptionDetailed(f) = &frames[0] else {
                panic!()
            };
            assert_eq!(f.text.text, format!("session-{index}"));
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn response_event_and_total_transcript_budgets_fail_closed() {
    for mode in 0..2 {
        let(origin,peer)=peer(move|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;let mut stream=respond.send_response(response(),false).unwrap();let mut bytes=event(json!({"type":"unknown"}));for _ in 0..3{bytes.extend(event(json!({"type":"conversation.item.input_audio_transcription.delta","delta":"abcdefgh"})));}send(&mut stream,&bytes,true,16384).await;}).await;
        let c = config(&origin);
        let mut l = c.limits().clone();
        if mode == 0 {
            l.max_response_events = 2;
        } else {
            l.max_transcript_bytes = 10;
        }
        let app = SageMakerSttApp::new(
            c.with_limits(l).unwrap(),
            vec![0; 32],
            Arc::new(Output::default()),
        )
        .unwrap()
        .start()
        .unwrap();
        let r = app.finish().await;
        assert!(!r.is_success());
        assert_eq!(r.primary, Some(pipecat_aws_sagemaker_stt::Error::TooLarge));
        assert_joined(&r);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn encoded_startup_budget_rejects_before_app_or_socket_effects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let c = config(&origin);
    let mut limits = c.limits().clone();
    limits.duplex.startup_max_bytes = 16;
    assert_eq!(
        c.with_limits(limits).unwrap_err(),
        pipecat_aws_sagemaker_stt::Error::TooLarge
    );
    assert!(timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn output_setup_failure_and_deadline_close_owned_transport() {
    use tokio::io::AsyncReadExt;
    for blocked in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}/", listener.local_addr().unwrap());
        let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tokio::select! {accepted=listener.accept()=>{let(mut stream,_)=accepted.unwrap();let mut buf=[0;1024];loop{match timeout(Duration::from_secs(3),stream.read(&mut buf)).await.unwrap(){Ok(0)|Err(_)=>break,Ok(_)=>{}}}},_= &mut done_rx=>{}}
        });
        let output = Arc::new(Output {
            setup_error: !blocked,
            setup_block: blocked,
            ..Default::default()
        });
        let app = SageMakerSttApp::new(config(&origin), vec![0; 32], output)
            .unwrap()
            .start()
            .unwrap();
        let r = timeout(Duration::from_secs(8), app.finish()).await.unwrap();
        assert!(!r.is_success(), "{r:?}");
        assert_eq!(r.primary, Some(pipecat_aws_sagemaker_stt::Error::Output));
        assert!(r.output_cleaned);
        assert_eq!(r.progress.pcm_bytes_uploaded, 0);
        let _ = done_tx.send(());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn immutable_settings_update_is_rejected_atomically_by_real_processor() {
    use pipecat_core::{
        AudioRawFrame, Direction, Frame, Pipeline, PipelineConfig, ServiceUpdateSettingsFrame,
    };
    let(origin,peer)=peer(|request,mut respond|async move{let mut u=Upload::new(request);u.finish().await;assert_eq!(u.events[0]["session"]["input_audio_transcription"]["language"],"en-US");assert_pcm(&u.events,&[0;32]);let mut stream=respond.send_response(response(),false).unwrap();send(&mut stream,&event(json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"unchanged"})),true,4096).await;}).await;
    let processor =
        Arc::new(pipecat_aws_sagemaker_stt::SageMakerSttProcessor::new(config(&origin)).unwrap());
    let output = Arc::new(Output::default());
    let mut worker = Pipeline::new(vec![processor.clone(), output.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    let feed = async {
        sender
            .send(
                Frame::new(FrameData::SttUpdateSettings(Box::new(
                    ServiceUpdateSettingsFrame {
                        settings: std::collections::BTreeMap::from([
                            ("language".into(), json!("fr-FR")),
                            ("endpoint".into(), json!("evil")),
                        ]),
                        ..Default::default()
                    },
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        sender
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 32], 16000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while processor.progress().chunks_admitted == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        sender.end().await.unwrap();
    };
    let drain = async { while worker.recv().await.is_some() {} };
    tokio::join!(feed, drain);
    let report = worker.join().await.unwrap();
    assert!(report.total_errors >= 1);
    assert_eq!(processor.progress().pcm_bytes_uploaded, 32);
    {
        let frames = output.frames.lock().unwrap();
        let FrameData::TranscriptionDetailed(f) = &frames[0] else {
            panic!()
        };
        assert_eq!(f.language.as_deref(), Some("en-US"));
    }
    peer.await.unwrap();
}

#[tokio::test]
async fn cancellation_after_complete_upload_interrupts_response_drain() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut u = Upload::new(request);
        u.finish().await;
        let mut stream = respond.send_response(response(), false).unwrap();
        tx.send(()).unwrap();
        std::future::poll_fn(|cx| stream.poll_reset(cx))
            .await
            .unwrap();
    })
    .await;
    let app = SageMakerSttApp::new(config(&origin), vec![0; 3200], Arc::new(Output::default()))
        .unwrap()
        .start()
        .unwrap();
    rx.await.unwrap();
    app.cancel();
    let r = app.finish().await;
    assert_eq!(r.terminal, Terminal::Cancelled);
    assert_eq!(r.progress.pcm_bytes_uploaded, 3200);
    assert_joined(&r);
    peer.await.unwrap();
}
