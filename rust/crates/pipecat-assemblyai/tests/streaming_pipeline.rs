// Tungstenite fixes the handshake callback error to its full HTTP response type.
#![allow(clippy::result_large_err)]
use futures_util::{SinkExt, StreamExt};
use pipecat_assemblyai::*;
use pipecat_core::Pipeline;
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, LlmContextAssistantTurnFrame, PipelineConfig,
    PipelineWorker, UserTurnStrategy, VadSpeakingFrame,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};
async fn next(worker: &mut PipelineWorker) -> pipecat_core::DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
fn turn(text: &str) -> Value {
    json!({"type":"Turn","turn_order":0,"turn_is_formatted":true,"end_of_turn":true,"transcript":text,"end_of_turn_confidence":1.0,"words":[]})
}
fn term() -> Message {
    Message::Text(
        json!({"type":"Termination","audio_duration_seconds":1.0,"session_duration_seconds":1.2})
            .to_string()
            .into(),
    )
}
#[tokio::test]
async fn upstream_vad_context_settings_and_end_have_single_terminal_owner() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = AssemblyAiConfig::new("test-key");
    c.endpoint = format!("ws://{}/v3/ws", l.local_addr().unwrap());
    c.limits.response_timeout = Duration::from_secs(1);
    let server = tokio::spawn(async move {
        let mut p = accept_async(l.accept().await.unwrap().0).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(p.next().await.unwrap().unwrap().to_text().unwrap())
                .unwrap(),
            json!({"type":"UpdateConfiguration","agent_context":"What is your name?"})
        );
        assert!(matches!(p.next().await,Some(Ok(Message::Binary(b))) if b.len()==1600));
        assert_eq!(
            serde_json::from_str::<Value>(p.next().await.unwrap().unwrap().to_text().unwrap())
                .unwrap(),
            json!({"type":"ForceEndpoint"})
        );
        p.send(Message::Text(
            turn("Actual wire transcript").to_string().into(),
        ))
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(p.next().await.unwrap().unwrap().to_text().unwrap())
                .unwrap(),
            json!({"type":"Terminate"})
        );
        p.send(term()).await.unwrap();
    });
    let processor =
        Arc::new(AssemblyAiSttProcessor::new(AssemblyAiClient::new(c).unwrap()).with_user_id("u"));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut start = false;
    while !start {
        start = matches!(next(&mut worker).await.frame.data, FrameData::Start(_));
    }
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextAssistantTurn(
                LlmContextAssistantTurnFrame {
                    text: "What is your name?".into(),
                    timestamp: "now".into(),
                },
            )),
            Direction::Upstream,
        )
        .await
        .unwrap();
    // Wait until the normal upstream turn was admitted before scheduling audio.
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::LlmContextAssistantTurn(_)
        ) {
            break;
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 1600], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    loop {
        if matches!(next(&mut worker).await.frame.data, FrameData::InputAudio(_)) {
            break;
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.05,
                timestamp: 1.0,
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    loop {
        match next(&mut worker).await.frame.data {
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "Actual wire transcript");
                assert_eq!(t.user_id, "u");
                assert!(t.finalized);
                assert!(chrono::DateTime::parse_from_rfc3339(&t.timestamp).is_ok());
                break;
            }
            FrameData::ProposedUserStartedSpeaking | FrameData::ProposedUserStoppedSpeaking => {
                panic!("Pipecat mode must not propose turns")
            }
            _ => (),
        }
    }
    worker
        .sender()
        .send(Frame::end(), Direction::Downstream)
        .await
        .unwrap();
    let mut ends = 0;
    while let Some(f) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        if matches!(f.frame.data, FrameData::End(_)) {
            ends += 1;
        }
    }
    assert_eq!(ends, 1);
    worker.join().await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn provider_mode_recommends_external_strategies_and_never_injects_interruption() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = AssemblyAiConfig::new("test-key");
    c.endpoint = format!("ws://{}/v3/ws", l.local_addr().unwrap());
    c.vad_force_turn_endpoint = false;
    c.should_interrupt = false;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut p = accept_async(l.accept().await.unwrap().0).await.unwrap();
        rx.await.unwrap();
        p.send(Message::Text(
            json!({"type":"SpeechStarted","timestamp":0})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        p.send(Message::Text(turn("Hello").to_string().into()))
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(p.next().await.unwrap().unwrap().to_text().unwrap())
                .unwrap(),
            json!({"type":"Terminate"})
        );
        p.send(term()).await.unwrap();
    });
    let mut worker = Pipeline::new(vec![Arc::new(AssemblyAiSttProcessor::new(
        AssemblyAiClient::new(c).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let mut metadata = false;
    let mut start = false;
    while !metadata || !start {
        match next(&mut worker).await.frame.data {
            FrameData::Start(_) => start = true,
            FrameData::SttMetadata(m) => {
                metadata = true;
                let s = m.base.user_turn_strategies.unwrap();
                assert_eq!(
                    s.start,
                    Some(vec![UserTurnStrategy::External {
                        enable_interruptions: false
                    }])
                );
                assert_eq!(s.stop, s.start);
            }
            _ => (),
        }
    }
    tx.send(()).unwrap();
    let mut output = vec![];
    let mut stops = 0;
    while stops < 2 {
        let f = next(&mut worker).await;
        if matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking) {
            stops += 1;
        }
        assert!(!matches!(
            f.frame.data,
            FrameData::Interruption
                | FrameData::UserStartedSpeaking
                | FrameData::UserStoppedSpeaking
        ));
        output.push(f);
    }
    let downstream: Vec<_> = output
        .iter()
        .filter(|f| f.direction == Direction::Downstream)
        .map(|f| &f.frame.data)
        .collect();
    assert!(matches!(
        downstream[0],
        FrameData::ProposedUserStartedSpeaking
    ));
    assert!(matches!(downstream[1],FrameData::Transcription(t) if t.finalized));
    assert!(matches!(
        downstream[2],
        FrameData::ProposedUserStoppedSpeaking
    ));
    worker
        .sender()
        .send(Frame::end(), Direction::Downstream)
        .await
        .unwrap();
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn cancel_aborts_pending_transcripts_and_closes_without_provider_terminate() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = AssemblyAiConfig::new("key");
    c.endpoint = format!("ws://{}/v3/ws", l.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut p = accept_async(l.accept().await.unwrap().0).await.unwrap();
        rx.await.unwrap();
        let _ = p
            .send(Message::Text(turn("too late").to_string().into()))
            .await;
        while let Some(msg) = p.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Text(_) | Message::Binary(_)) => panic!("cancel flushed data"),
                _ => (),
            }
        }
    });
    let mut worker = Pipeline::new(vec![Arc::new(AssemblyAiSttProcessor::new(
        AssemblyAiClient::new(c).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    while !matches!(next(&mut worker).await.frame.data, FrameData::Start(_)) {}
    worker
        .sender()
        .send(Frame::cancel("test cancellation"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if matches!(next(&mut worker).await.frame.data, FrameData::Cancel(_)) {
            break;
        }
    }
    let _ = tx.send(());
    while let Some(f) = worker.recv().await {
        assert!(!matches!(f.frame.data, FrameData::Transcription(_)));
    }
    worker.join().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn inherited_and_explicit_sample_rate_query_contract() {
    for explicit in [None, Some(16000)] {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut c = AssemblyAiConfig::new("key");
        c.endpoint = format!("ws://{}/v3/ws", l.local_addr().unwrap());
        c.sample_rate = explicit;
        let server = tokio::spawn(async move {
            let mut p=tokio_tungstenite::accept_hdr_async(l.accept().await.unwrap().0,move|r:&tokio_tungstenite::tungstenite::handshake::server::Request,s:tokio_tungstenite::tungstenite::handshake::server::Response|{let q:std::collections::BTreeMap<_,_>=url::form_urlencoded::parse(r.uri().query().unwrap().as_bytes()).into_owned().collect();assert_eq!(q["sample_rate"],explicit.unwrap_or(8000).to_string());Ok(s)}).await.unwrap();
            assert!(matches!(p.next().await, Some(Ok(Message::Text(_)))));
            p.send(term()).await.unwrap();
        });
        let mut cfg = PipelineConfig::default();
        cfg.start.audio_in_sample_rate = 8000;
        let mut worker = Pipeline::new(vec![Arc::new(AssemblyAiSttProcessor::new(
            AssemblyAiClient::new(c).unwrap(),
        ))])
        .start(cfg)
        .await
        .unwrap();
        while !matches!(next(&mut worker).await.frame.data, FrameData::Start(_)) {}
        worker
            .sender()
            .send(Frame::end(), Direction::Downstream)
            .await
            .unwrap();
        while worker.recv().await.is_some() {}
        worker.join().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn user_audio_identity_is_retained_without_provider_diarization() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = AssemblyAiConfig::new("key");
    c.endpoint = format!("ws://{}/v3/ws", l.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut p = accept_async(l.accept().await.unwrap().0).await.unwrap();
        assert!(matches!(p.next().await, Some(Ok(Message::Binary(_)))));
        p.send(Message::Text(turn("named user").to_string().into()))
            .await
            .unwrap();
        assert!(matches!(p.next().await, Some(Ok(Message::Text(_)))));
        p.send(term()).await.unwrap();
    });
    let mut worker = Pipeline::new(vec![Arc::new(AssemblyAiSttProcessor::new(
        AssemblyAiClient::new(c).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    while !matches!(next(&mut worker).await.frame.data, FrameData::Start(_)) {}
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserAudio(pipecat_core::UserAudioRawFrame {
                audio: AudioRawFrame::new(vec![0; 1600], 16000, 1).unwrap(),
                user_id: "speaker-B".into(),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    loop {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            assert_eq!(t.user_id, "speaker-B");
            break;
        }
    }
    worker
        .sender()
        .send(Frame::end(), Direction::Downstream)
        .await
        .unwrap();
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
    server.await.unwrap();
}
