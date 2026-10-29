//! Offline fixture through the adopted STT settings; no device or Hepta admission.
use pipecat_core::AudioRawFrame;
use pipecat_hepta::local_speech::LocalSpeechConfig;
use pipecat_whisper::{WhisperConfig, WhisperService};
use serde_json::json;
use std::{io::Read, time::Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: stt_fixture LOCAL_SPEECH_CONFIG PCM16_WAV".into());
    }
    let config: LocalSpeechConfig = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    if pipecat_piper::sha256_file(&config.whisper_model.path)? != config.whisper_model.sha256 {
        return Err("explicit Whisper model SHA-256 mismatch".into());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&args[1])?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err("fixture exceeds 1 MiB".into());
    }
    let wav = pipecat_audio::read_pcm16_wav(&bytes)?;
    if wav.format.sample_rate() != 16000 || wav.format.channels() != 1 {
        return Err("fixture must be 16 kHz mono PCM16".into());
    }
    let audio = AudioRawFrame::new(wav.pcm.to_vec(), 16000, 1)?;
    let start = Instant::now();
    let service = WhisperService::load(WhisperConfig::new(&config.whisper_model.path)).await?;
    let load_us = start.elapsed().as_micros();
    let start = Instant::now();
    let transcript = service
        .submit(&audio, config.whisper_settings.clone())?
        .result()
        .await;
    let stt_us = start.elapsed().as_micros();
    service.shutdown().await?;
    println!(
        "{}",
        json!({"scope":"offline_wav_stt_no_device_or_hepta_admission",
        "model_sha256":config.whisper_model.sha256,"wav_sha256":pipecat_piper::sha256_bytes(&bytes),
        "settings":config.whisper_settings,"load_us":load_us,"stt_us":stt_us,"transcript":transcript?})
    );
    Ok(())
}
