//! Credential-free native WebRTC smoke test over operating-system loopback UDP.
use pipecat_core::AudioRawFrame;
use pipecat_webrtc::{SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SmallWebRtcConfig {
        localhost_only: true,
        ..Default::default()
    };
    let codec = config.audio_codec.unwrap();
    let caller = SmallWebRtcConnection::new(config.clone()).await?;
    let answerer = SmallWebRtcConnection::new(config).await?;
    let offer = caller.create_offer().await?;
    let answer = answerer.answer_offer(offer.sdp).await?;
    caller.accept_answer(answer).await?;
    caller.wait_data_channel(Duration::from_secs(5)).await?;
    caller
        .send_app_message(
            &json!({"label":"rtvi-ai", "type":"client-ready", "id":"loopback-1", "data":{}}),
        )
        .await?;
    let samples = (0..codec.sample_rate() / 50)
        .map(|i| {
            ((i as f32 * std::f32::consts::TAU * 440.0 / codec.sample_rate() as f32).sin()
                * 8_000.0) as i16
        })
        .collect::<Vec<_>>();
    let audio = AudioRawFrame::new(
        pipecat_audio::encode_pcm16_le(&samples),
        codec.sample_rate(),
        1,
    )?;
    for _ in 0..5 {
        caller.send_audio(audio.clone()).await?;
    }
    caller.flush(Duration::from_secs(5)).await?;
    let mut audio_packets = 0;
    let mut message_received = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        while audio_packets < 5 || !message_received {
            match answerer.recv_event().await {
                Some(TransportEvent::AppMessage(value)) => {
                    message_received = value["type"] == "client-ready";
                }
                Some(TransportEvent::Audio(audio)) => {
                    assert_eq!(audio.frame.num_frames(), codec.sample_rate() as usize / 50);
                    audio_packets += 1;
                }
                Some(TransportEvent::Error(error)) => return Err(std::io::Error::other(error)),
                None => {
                    return Err(std::io::Error::other(
                        "transport closed before receiving the smoke test",
                    ))
                }
                _ => {}
            }
        }
        Ok(())
    })
    .await??;
    println!(
        "{}",
        json!({"transport":"SmallWebRTC", "network":"real loopback UDP", "codec":codec, "sample_rate":codec.sample_rate(), "rtvi_received":message_received, "audio_packets_received":audio_packets})
    );
    caller.close().await?;
    answerer.close().await?;
    Ok(())
}
