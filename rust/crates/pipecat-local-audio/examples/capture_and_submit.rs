//! Explicit opt-in microphone capture followed by output callback submission.
//! Run with optional INPUT_DEVICE_ID OUTPUT_DEVICE_ID from `list_devices`.
use pipecat_audio::PcmFormat;
use pipecat_core::FrameData;
use pipecat_local_audio::{AudioLimits, DeviceSelector, LocalAudioInput, LocalAudioOutput};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let input_device = args.next().map(DeviceSelector::Id).unwrap_or_default();
    let output_device = args.next().map(DeviceSelector::Id).unwrap_or_default();
    if args.next().is_some() {
        return Err("usage: capture_and_submit [INPUT_DEVICE_ID] [OUTPUT_DEVICE_ID]".into());
    }
    let format = PcmFormat::new(16_000, 1)?;
    let input = LocalAudioInput::new(input_device, AudioLimits::default())?;
    let output = LocalAudioOutput::new(output_device, AudioLimits::default())?;
    // Open both before activating capture so unsupported output settings fail early.
    output.open(format)?;
    input.open(format)?;
    let mut capture = input.capture_stream()?;
    let mut audio = Vec::with_capacity(20);
    for _ in 0..20 {
        let frame =
            tokio::time::timeout(std::time::Duration::from_secs(5), capture.next_frame()).await??;
        let FrameData::InputAudio(frame) = frame.data else {
            unreachable!()
        };
        audio.push(frame);
    }
    drop(capture);
    input.close();
    for frame in audio {
        let receipt = output.write_audio(&frame).await?;
        println!(
            "Submitted {} PCM frames to native callbacks; playback unconfirmed",
            receipt.frames
        );
    }
    // Allow a brief best-effort device tail before closing. A wall-clock delay is
    // not converted into a Played receipt and does not prove acoustic playback.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    output.close();
    Ok(())
}
