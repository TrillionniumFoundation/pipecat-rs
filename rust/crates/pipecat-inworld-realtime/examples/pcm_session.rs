//! Recorded mono PCM16 → native Inworld speech → recorded mono PCM16.
//! This example never opens a microphone, speaker, browser or public listener.
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, ProcessorContext,
    ProcessorError,
};
use pipecat_inworld_realtime::{InworldApp, InworldConfig, SessionProperties};
use pipecat_services::Context;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

struct PcmOutput(Mutex<(std::fs::File, usize)>);
#[async_trait]
impl FrameProcessor for PcmOutput {
    fn name(&self) -> &str {
        "RecordedPcmOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::TtsAudio(audio) = &frame.data {
                let mut output = self
                    .0
                    .lock()
                    .map_err(|_| ProcessorError::fatal("output lock failed"))?;
                if output.1.saturating_add(audio.audio.len()) > 16 * 1024 * 1024 {
                    return Err(ProcessorError::fatal("recorded output exceeds 16 MiB"));
                }
                output
                    .0
                    .write_all(&audio.audio)
                    .map_err(|_| ProcessorError::fatal("recorded output write failed"))?;
                output.1 += audio.audio.len();
            }
        }
        ctx.push(frame, direction).await
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("pcm_session --manual|--server-vad INPUT.pcm OUTPUT.pcm\npcm_session --check-config\nExplicit INWORLD_API_KEY required (already encoded Basic credential). Mono signed little-endian PCM16 at 24000 Hz, input <= 8 MiB, new output file only. Server-VAD input must include end-of-speech silence. Uses InworldApp and records generated speech; no audible playback. Network occurs only in session mode.");
        return Ok(());
    }
    let check = args == ["--check-config"];
    if !check && (args.len() != 3 || !matches!(args[0].as_str(), "--manual" | "--server-vad")) {
        return Err("invalid arguments; use --help".into());
    }
    let key = std::env::var("INWORLD_API_KEY")
        .map_err(|_| "INWORLD_API_KEY must be explicitly configured")?;
    let mut config = InworldConfig::new(key);
    if !check && args[0] == "--manual" {
        config.session = SessionProperties::manual();
    }
    config.validate()?;
    if check {
        println!("Inworld PCM configuration valid; no connection opened");
        return Ok(());
    }
    let metadata = std::fs::metadata(&args[1]).map_err(|_| "input file unavailable")?;
    if metadata.len() == 0 || metadata.len() > 8 * 1024 * 1024 || !metadata.len().is_multiple_of(2)
    {
        return Err("input must contain 1..8 MiB of aligned PCM16 samples".into());
    }
    let mut pcm = Vec::new();
    std::fs::File::open(&args[1])
        .map_err(|_| "input file read failed")?
        .take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut pcm)
        .map_err(|_| "input file read failed")?;
    if pcm.len() as u64 != metadata.len() {
        return Err("input changed while reading".into());
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[2])
        .map_err(|_| "output must be a new writable file")?;
    let app = InworldApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(PcmOutput(Mutex::new((file, 0)))),
    )?;
    let manual = args[0] == "--manual";
    let mut worker = app.start().await?.worker;
    let sender = worker.sender();
    let feed = async {
        for chunk in pcm.chunks(2880) {
            sender
                .send(
                    Frame::new(FrameData::InputAudio(AudioRawFrame {
                        audio: chunk.to_vec(),
                        sample_rate: 24_000,
                        num_channels: 1,
                        context_id: None,
                    })),
                    Direction::Downstream,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        if manual {
            sender
                .send(
                    Frame::new(FrameData::UserStoppedSpeaking),
                    Direction::Downstream,
                )
                .await?;
        }
        Ok::<_, pipecat_core::PipelineError>(())
    };
    let receive = async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::LlmResponseEnd => return Ok(()),
                FrameData::Error(_) => return Err("provider session failed"),
                _ => {}
            }
        }
        Err("session closed before response")
    };
    let outcome = tokio::time::timeout(Duration::from_secs(300), async {
        tokio::try_join!(
            async { feed.await.map_err(|_| "input feed failed") },
            receive
        )
    })
    .await;
    if matches!(outcome, Ok(Ok(_))) {
        sender.end().await?;
    } else {
        let _ = sender.cancel("recorded session failed or timed out").await;
    }
    let joined = worker.collect().await;
    if !matches!(outcome, Ok(Ok(_))) {
        return Err("recorded session failed or timed out".into());
    }
    let (_, report) = joined?;
    if !report.errors.is_empty() || report.total_errors != 0 {
        return Err("recorded session ended with pipeline or cleanup errors".into());
    }
    println!("Recorded response complete; generated audio is not playback evidence");
    Ok(())
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Configuration and runtime errors contain no credentials, URLs or provider payloads.
        eprintln!("{error}");
        std::process::exit(1);
    }
}
