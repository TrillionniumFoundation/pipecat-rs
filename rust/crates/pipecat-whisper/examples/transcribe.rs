//! Offline WAV transcription: cargo run -p pipecat-whisper --example transcribe -- MODEL WAV [LANGUAGE]
use pipecat_core::AudioRawFrame;
use pipecat_whisper::{WhisperConfig, WhisperService, WhisperSettings};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let model = arguments
        .next()
        .ok_or("usage: transcribe MODEL WAV [LANGUAGE]")?;
    let wav = arguments
        .next()
        .ok_or("usage: transcribe MODEL WAV [LANGUAGE]")?;
    let mut settings = WhisperSettings::default();
    if let Some(language) = arguments.next() {
        let language = language.to_str().ok_or("language must be UTF-8")?;
        settings.language = if language == "auto" {
            None
        } else {
            Some(language.parse()?)
        };
    }
    if arguments.next().is_some() {
        return Err("too many arguments".into());
    }
    let bytes = std::fs::read(wav)?;
    let wave = pipecat_audio::read_pcm16_wav(&bytes)?;
    let audio = AudioRawFrame::new(
        wave.pcm.to_vec(),
        wave.format.sample_rate(),
        wave.format.channels(),
    )?;
    let service = WhisperService::load(WhisperConfig::new(model)).await?;
    let transcript = service.submit(&audio, settings)?.result().await;
    service.shutdown().await?;
    println!("{}", serde_json::to_string_pretty(&transcript?)?);
    Ok(())
}
