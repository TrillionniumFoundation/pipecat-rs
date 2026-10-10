//! Provider-reported source fragments cross the real TTS/playback boundary.
use pipecat_cli::playback::PlaybackOutput;
use pipecat_core::{Direction, Frame, FrameData, Pipeline, PipelineConfig, TtsTextInputFrame};
use pipecat_services::{Content, Context, Role};
use pipecat_streaming_speech::{CartesiaClient, CartesiaConfig, CartesiaTtsProcessor};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;

async fn confirm_source(inputs: Vec<TtsTextInputFrame>, reported_words: &[&str], expected: &str) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("synthetic-only", "voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    let expected_synthesis: String = inputs
        .iter()
        .map(|input| input.synthesis_text.as_str())
        .collect();
    let words: Vec<String> = reported_words.iter().map(|word| (*word).into()).collect();
    let provider = tokio::spawn(async move {
        let mut session =
            WebSocketSession::accept(listener.accept().await.unwrap().0, SessionConfig::default())
                .await
                .unwrap();
        let mut transcript = String::new();
        let mut context_id = None;
        while let Some(packet) = session.recv().await {
            let Packet::Text(text) = packet else {
                panic!("TTS requests must use JSON text");
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            let id = value["context_id"].as_str().unwrap().to_owned();
            if let Some(previous) = &context_id {
                assert_eq!(&id, previous, "one response must retain its context");
            }
            context_id = Some(id.clone());
            transcript.push_str(value["transcript"].as_str().unwrap());
            if value["continue"] == false {
                assert_eq!(transcript, expected_synthesis);
                let starts: Vec<f64> = (0..words.len())
                    .map(|index| index as f64 * 0.000_001)
                    .collect();
                for message in [
                    json!({"type":"chunk","context_id":id,"data":"AAABAP//AgA="}),
                    json!({"type":"timestamps","context_id":id,"word_timestamps":{"words":words,"start":starts}}),
                    json!({"type":"done","context_id":id}),
                ] {
                    session
                        .sender()
                        .send(Packet::Text(message.to_string()))
                        .await
                        .unwrap();
                }
                let _ = session.finish().await;
                return id;
            }
        }
        panic!("TTS context ended without finalization");
    });
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let shared = pair.context();
    let (output, _channels) = PlaybackOutput::new(pair.assistant());
    let mut worker = Pipeline::new(vec![
        Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(config).unwrap(),
        )),
        output.clone(),
        pair.assistant(),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    for data in std::iter::once(FrameData::LlmResponseStart)
        .chain(inputs.into_iter().map(FrameData::TtsTextInput))
        .chain(std::iter::once(FrameData::LlmResponseEnd))
    {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    let finished_id = tokio::time::timeout(Duration::from_secs(3), async {
        let mut audio = Vec::new();
        while let Some(frame) = worker.recv().await {
            assert!(
                !matches!(frame.frame.data, FrameData::Error(_)),
                "native synthesis must succeed: {:?}",
                frame.frame.data
            );
            match frame.frame.data {
                FrameData::TtsAudio(pcm) => {
                    assert_eq!(pcm.sample_rate, 24000);
                    assert_eq!(pcm.num_channels, 1);
                    audio.extend_from_slice(&pcm.audio);
                }
                FrameData::TtsStopped(stop) => {
                    assert_eq!(audio, [0, 0, 1, 0, 255, 255, 2, 0]);
                    return stop.context_id.unwrap();
                }
                _ => {}
            }
        }
        panic!("pipeline ended before synthesis completed");
    })
    .await
    .unwrap();
    let id = provider.await.unwrap();
    assert_eq!(finished_id, id);
    assert!(
        shared.snapshot().unwrap().messages.is_empty(),
        "provider reports and synthesis completion must not commit speech"
    );
    output.acknowledge(id.clone()).await.unwrap();
    let messages = shared.snapshot().unwrap().messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::Assistant);
    assert_eq!(messages[0].content, Some(Content::Text(expected.into())));
    output.acknowledge(id).await.unwrap();
    assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

fn aggregate(text: &str) -> TtsTextInputFrame {
    let mut input = TtsTextInputFrame::new(text);
    input.includes_inter_frame_spaces = false;
    input
}

#[tokio::test]
async fn acknowledged_source_keeps_internal_whitespace_and_separate_aggregate_boundaries() {
    confirm_source(
        vec![aggregate("Hello  from\tRust!"), aggregate("Next item.")],
        &["Hello", "from", "Rust!", "Next", "item."],
        "Hello  from\tRust! Next item.",
    )
    .await;
}

#[tokio::test]
async fn acknowledged_transform_preserves_original_markup_without_synthesis_expansion() {
    let mut price = aggregate("Pay $5");
    price.synthesis_text = "Pay five dollars".into();
    price.original_text = "Pay <b>$5</b>".into();
    confirm_source(
        vec![price, aggregate("Thank you.")],
        &["Pay", "five", "dollars", "Thank", "you."],
        "Pay <b>$5</b> Thank you.",
    )
    .await;
}

#[tokio::test]
async fn acknowledged_audio_does_not_promote_unreported_suffix_to_spoken_history() {
    confirm_source(
        vec![aggregate("Heard unreported suffix.")],
        &["Heard"],
        "Heard",
    )
    .await;
}

#[tokio::test]
async fn leading_suppressed_transform_keeps_its_aggregate_boundary() {
    let mut price = aggregate("$5");
    price.synthesis_text = "five dollars".into();
    price.original_text = "<b>$5</b>".into();
    confirm_source(
        vec![aggregate("Total price:"), price],
        &["Total", "price:", "five", "dollars"],
        "Total price: <b>$5</b>",
    )
    .await;
}

#[tokio::test]
async fn explicit_contiguous_source_parts_do_not_invent_aggregate_spaces() {
    let mut first = aggregate("some");
    first.includes_inter_frame_spaces = true;
    let mut second = aggregate("thing");
    second.includes_inter_frame_spaces = true;
    confirm_source(vec![first, second], &["some", "thing"], "something").await;
}
