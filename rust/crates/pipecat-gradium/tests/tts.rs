//! Native synthetic WebSocket + pipeline fixtures, never hosted provider calls.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gradium::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("TTS fixture stalled")
}
async fn fixture() -> (
    GradiumTtsConfig,
    mpsc::Receiver<Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GradiumTtsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.allow_loopback = true;
    let (tx, rx) = mpsc::channel(32);
    let task = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut synthesis_text = String::new();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            tx.send(value.clone()).await.unwrap();
            if value["type"] == "text" {
                synthesis_text.push_str(value["text"].as_str().unwrap());
            }
            if value["type"] == "end_of_stream" {
                let id = &value["client_req_id"];
                for event in [
                    json!({"type":"text","client_req_id":id,"text":synthesis_text,"start_s":0.0}),
                    json!({"type":"audio","client_req_id":id,"audio":STANDARD.encode([1])}),
                    json!({"type":"audio","client_req_id":id,"audio":STANDARD.encode([2,3,4])}),
                    json!({"type":"end_of_stream","client_req_id":id}),
                ] {
                    socket
                        .send(Message::Text(event.to_string().into()))
                        .await
                        .unwrap();
                }
            }
        }
    });
    (config, rx, task)
}
#[test]
fn pinned_configuration_is_tts_specific_and_atomic() {
    let mut config = GradiumTtsConfig::new("do-not-log");
    config.settings.json_config = Some(" {\"temperature\": 0.6} ".into());
    let setup = config.setup_message("one");
    assert_eq!(
        setup,
        json!({"type":"setup","client_req_id":"one","voice_id":"_6Aslh2DxfmnRLmP","output_format":"pcm","close_ws_on_eos":false,"json_config":" {\"temperature\": 0.6} "})
    );
    let client = GradiumTtsClient::new(config.clone()).unwrap();
    let invalid = json!({"voice":"new","json_config":{"temperature":0.6}});
    assert!(config.settings.merge(invalid.as_object().unwrap()).is_err());
    assert_eq!(client.config().settings.voice, "_6Aslh2DxfmnRLmP");
    assert!(!format!("{client:?}").contains("do-not-log"));
    for endpoint in [
        "ws://api.gradium.ai/api/speech/tts",
        "wss://127.0.0.1/tts",
        "wss://api.gradium.ai/tts?key=secret",
        "wss://name:secret@api.gradium.ai/tts",
    ] {
        let mut config = config.clone();
        config.endpoint = endpoint.into();
        assert!(GradiumTtsClient::new(config).is_err());
    }
    config.sample_rate = 24000;
    assert!(GradiumTtsClient::new(config).is_err());
}
#[tokio::test]
async fn real_wire_finish_preserves_text_order_partial_samples_and_one_terminal() {
    let (config, mut wire, server) = fixture().await;
    let client = GradiumTtsClient::new(config).unwrap();
    let mut stream = client.stream("first").unwrap();
    let sender = stream.sender();
    sender.send_text("Hello.").await.unwrap();
    sender.finish().await.unwrap();
    assert_eq!(sender.try_finish(), Err(GradiumError::Closed));
    assert_eq!(sender.try_send_text("late"), Err(GradiumError::Closed));
    assert_eq!(bounded(wire.recv()).await.unwrap()["type"], "setup");
    assert_eq!(
        bounded(wire.recv()).await.unwrap(),
        json!({"type":"text","client_req_id":"first","text":"Hello."})
    );
    assert_eq!(
        bounded(wire.recv()).await.unwrap(),
        json!({"type":"end_of_stream","client_req_id":"first"})
    );
    let mut events = vec![];
    while let Some(event) = bounded(stream.next()).await {
        events.push(event.unwrap());
    }
    assert_eq!(events.len(), 3);
    let GradiumTtsEvent::Audio(audio) = &events[1] else {
        panic!("missing audio");
    };
    assert_eq!(audio.audio, [1, 2, 3, 4]);
    assert_eq!((audio.sample_rate, audio.num_channels), (48000, 1));
    assert!(matches!(&events[2], GradiumTtsEvent::Done { context_id } if context_id == "first"));
    assert!(client.stream("first").is_err());
    bounded(client.shutdown()).await;
    bounded(server).await.unwrap();
}
#[tokio::test]
async fn real_native_pipeline_orders_generated_audio_and_response_end() {
    let (config, _wire, server) = fixture().await;
    let processor = Arc::new(GradiumTtsProcessor::new(
        GradiumTtsClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: 48000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::LlmText(TextFrame::llm("Hello."))),
        end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let pos =
        |test: fn(&FrameData) -> bool| frames.iter().position(|f| test(&f.frame.data)).unwrap();
    let started = pos(|f| matches!(f, FrameData::TtsStarted(_)));
    let audio = pos(|f| matches!(f, FrameData::TtsAudio(_)));
    let stopped = pos(|f| matches!(f, FrameData::TtsStopped(_)));
    let ended = frames.iter().position(|f| f.frame.id == end_id).unwrap();
    assert!(started < audio && audio < stopped && stopped < ended);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .count(),
        1
    );
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn pipeline_end_flushes_a_pending_partial_sentence() {
    let (config, mut wire, server) = fixture().await;
    let processor = Arc::new(GradiumTtsProcessor::new(
        GradiumTtsClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: 48000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("Hello")),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(bounded(wire.recv()).await.unwrap()["type"], "setup");
    assert_eq!(bounded(wire.recv()).await.unwrap()["text"], "Hello");
    assert_eq!(bounded(wire.recv()).await.unwrap()["type"], "end_of_stream");
    let audio = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
        .unwrap();
    let stopped = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .unwrap();
    let end = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(audio < stopped && stopped < end);
    bounded(server).await.unwrap();
}
