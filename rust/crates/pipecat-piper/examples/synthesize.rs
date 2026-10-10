//! Explicit offline use: synthesize MANIFEST_JSON OUTPUT_WAV TEXT
use pipecat_piper::{PiperConfig, PiperService, PiperSettings};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: synthesize MANIFEST_JSON OUTPUT_WAV TEXT".into());
    }
    let config: PiperConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let service = PiperService::load(config).await?;
    let result = service
        .submit(&args[2], "offline-example", PiperSettings::default())?
        .result()
        .await;
    service.drain().await?;
    let audio = result?;
    let wav = pipecat_audio::pcm_to_wav(&audio.pcm, audio.sample_rate, audio.num_channels)?;
    std::fs::write(&args[1], wav)?;
    println!(
        "{} PCM samples, {} Hz, voice {}",
        audio.pcm.len() / 2,
        audio.sample_rate,
        audio.voice
    );
    Ok(())
}
