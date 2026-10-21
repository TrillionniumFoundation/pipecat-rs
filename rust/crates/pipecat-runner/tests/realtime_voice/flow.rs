use crate::realtime_support::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_audio::SincResampler;
use pipecat_browser_client::protocol;
use pipecat_services::Content;
use serde_json::json;

#[tokio::test]
async fn http_capability_actual_browser_ready_and_text_use_one_native_inference() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    assert_eq!(start["voiceMode"], "openai-realtime");
    assert_eq!(start["inputSampleRate"], 24000);
    assert_eq!(start["playbackAcknowledgements"], false);
    let mut browser = host.browser(&start, 51).await;
    let init = provider.command().await;
    assert_eq!(
        init["session"]["audio"]["input"]["format"],
        json!({"type":"audio/pcm","rate":24000})
    );
    assert_eq!(
        init["session"]["audio"]["input"]["turn_detection"]["type"],
        "server_vad"
    );
    assert!(init["session"]["tools"].is_null());
    browser.ready().await;
    browser.text("hello native realtime").await;
    let item = provider.inference().await;
    assert_eq!(item["type"], "conversation.item.create");
    assert_eq!(item["item"]["content"][0]["text"], "hello native realtime");
    provider.quiet().await;
    response(&provider, "r1", "Hello once.", &[1, 0, 2, 0, 3, 0]).await;
    let mut pcm = vec![];
    let mut output = 0;
    let mut stopped = false;
    while pcm.len() < 6 || !stopped {
        match browser.recv().await {
            Incoming::Audio(bytes) => pcm.extend(bytes),
            Incoming::Json(value) => match value["type"].as_str() {
                Some("bot-output") => {
                    assert_eq!(value["data"]["text"], "Hello once.");
                    assert_eq!(value["data"]["spoken"], false);
                    output += 1;
                }
                Some("bot-llm-stopped") => stopped = true,
                _ => {}
            },
            _ => {}
        }
    }
    assert_eq!(pcm, [1, 0, 2, 0, 3, 0]);
    assert_eq!(output, 1);
    browser
        .message(json!({"label":"rtvi-ai","id":"done","type":"disconnect-bot"}))
        .await;
    let report = host.report().await;
    let _ = browser.socket.finish().await;
    assert!(report.successful);
    assert_eq!(report.generated_context.messages.len(), 2);
    assert_eq!(
        report.generated_context.messages[0].content,
        Some(Content::Text("hello native realtime".into()))
    );
    assert_eq!(
        report.generated_context.messages[1].content,
        Some(Content::Text("Hello once.".into()))
    );
    host.close().await;
    provider.joined().await;
}

#[tokio::test]
async fn awkward_48khz_chunks_match_stateful_24khz_pcm_and_upstream_user_history() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 52).await;
    provider.command().await;
    browser.ready().await;
    let samples: Vec<f32> = (0..4096).map(|i| ((i % 97) as f32 - 48.) / 100.).collect();
    let mut resampler = SincResampler::new(48000, 24000, 1).unwrap();
    let mut expected = vec![];
    for chunk in samples.chunks(137) {
        let body = protocol::decode(&protocol::encode_audio(chunk, 48000))
            .unwrap()
            .body
            .unwrap();
        let protocol::frame::Body::Audio(audio) = body else {
            unreachable!()
        };
        expected.extend(resampler.process(&audio.audio).unwrap());
        browser.audio(chunk, 48000).await;
    }
    let mut actual = vec![];
    while actual.len() < expected.len() {
        let command = provider.command().await;
        assert_eq!(command["type"], "input_audio_buffer.append");
        actual.extend(STANDARD.decode(command["audio"].as_str().unwrap()).unwrap());
    }
    assert_eq!(actual, expected);
    provider
        .event(
            json!({"type":"input_audio_buffer.speech_started","audio_start_ms":0,"item_id":"u1"}),
        )
        .await;
    provider
        .event(json!({"type":"input_audio_buffer.speech_stopped","audio_end_ms":85,"item_id":"u1"}))
        .await;
    provider.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u1","content_index":0,"transcript":"spoken user"})).await;
    response(&provider, "voice", "Spoken answer.", &[8, 0, 9, 0]).await;
    let mut transcripts = 0;
    let mut stopped = false;
    while transcripts == 0 || !stopped {
        if let Incoming::Json(value) = browser.recv().await {
            if value["type"] == "user-transcription" {
                assert_eq!(value["data"]["text"], "spoken user");
                transcripts += 1;
            }
            if value["type"] == "bot-llm-stopped" {
                stopped = true;
            }
        }
    }
    assert_eq!(transcripts, 1);
    provider.quiet().await; // server VAD does not create a second local response
    browser
        .message(json!({"label":"rtvi-ai","id":"done","type":"disconnect-bot"}))
        .await;
    let report = host.report().await;
    let _ = browser.socket.finish().await;
    assert!(report.successful);
    assert_eq!(
        report
            .generated_context
            .messages
            .iter()
            .filter(|m| m.content == Some(Content::Text("spoken user".into())))
            .count(),
        1
    );
    assert_eq!(
        report
            .generated_context
            .messages
            .iter()
            .filter(|m| m.content == Some(Content::Text("Spoken answer.".into())))
            .count(),
        1
    );
    host.close().await;
    provider.joined().await;
}

#[tokio::test]
async fn interruption_cancels_once_without_playback_truncate_and_new_turn_works() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 53).await;
    provider.command().await;
    browser.ready().await;
    browser.text("first").await;
    provider.inference().await;
    provider.event(created("old")).await;
    provider.event(transcript("old", "old text")).await;
    loop {
        if let Incoming::Json(value) = browser.recv().await {
            if value["type"] == "bot-output" {
                break;
            }
        }
    }
    browser
        .socket
        .sender()
        .send(
            pipecat_runner::wire::packet(pipecat_protocol::protobuf::frame::Kind::Interruption(
                pipecat_protocol::protobuf::InterruptionFrame {
                    id: 700,
                    name: "interruption".into(),
                },
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provider.command().await["type"], "response.cancel");
    loop {
        if matches!(browser.recv().await, Incoming::Interrupted) {
            break;
        }
    }
    provider
        .event(transcript("old", "STALE MUST NOT APPEAR"))
        .await;
    provider.event(json!({"type":"response.output_audio.delta","response_id":"old","item_id":"item-old","output_index":0,"content_index":0,"delta":STANDARD.encode([99,0])})).await;
    provider.event(json!({"type":"response.done","response":{"id":"old","status":"cancelled","output":[]}})).await;
    provider.quiet().await; // no guessed conversation.item.truncate or second cancel
    browser.text("second").await;
    assert_eq!(
        provider.inference().await["type"],
        "conversation.item.create"
    );
    response(&provider, "new", "fresh", &[11, 0]).await;
    let mut heard = false;
    let mut stopped = false;
    while !heard || !stopped {
        match browser.recv().await {
            Incoming::Audio(bytes) => {
                assert_eq!(bytes, [11, 0]);
                heard = true;
            }
            Incoming::Json(value) => {
                assert!(!value.to_string().contains("STALE"));
                if value["type"] == "bot-llm-stopped" {
                    stopped = true;
                }
            }
            _ => {}
        }
    }
    browser
        .message(json!({"label":"rtvi-ai","id":"done","type":"disconnect-bot"}))
        .await;
    assert!(host.report().await.successful);
    let _ = browser.socket.finish().await;
    host.close().await;
    provider.joined().await;
}

#[tokio::test]
async fn sequential_sessions_do_not_reuse_context_or_provider_actor() {
    let mut provider = Provider::sessions(true, 2).await;
    let mut host = Host::new(provider.config.clone()).await;
    for id in [61, 62] {
        let start = host.start().await;
        let mut browser = host.browser(&start, id).await;
        provider.command().await;
        browser.ready().await;
        browser.text(&format!("session-{id}")).await;
        let item = provider.inference().await;
        assert_eq!(item["item"]["content"][0]["text"], format!("session-{id}"));
        response(&provider, "one", "answer", &[0, 0]).await;
        loop {
            if let Incoming::Json(value) = browser.recv().await {
                if value["type"] == "bot-llm-stopped" {
                    break;
                }
            }
        }
        browser
            .message(json!({"label":"rtvi-ai","id":"done","type":"disconnect-bot"}))
            .await;
        let report = host.report().await;
        let _ = browser.socket.finish().await;
        assert!(report.successful);
        assert_eq!(report.generated_context.messages.len(), 2);
        assert_eq!(host.runner.active_sessions(), 0);
    }
    host.close().await;
    provider.joined().await;
}

#[tokio::test]
async fn introduction_creates_only_one_inference() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::configured(provider.config.clone(), true).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 71).await;
    provider.command().await;
    browser.ready().await;
    let item = provider.inference().await;
    assert_eq!(item["type"], "conversation.item.create");
    assert!(item.to_string().contains("Please introduce yourself"));
    provider.quiet().await;
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
}
