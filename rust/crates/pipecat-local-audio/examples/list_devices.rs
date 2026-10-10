//! Read-only device discovery. This does not record or play audio.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let devices = pipecat_local_audio::list_devices()?;
    println!("{} native audio devices", devices.len());
    for device in devices {
        println!(
            "{}: {} [{}] input={} output={} default_input={} default_output={}",
            device.index,
            device.name,
            device.id,
            device.input,
            device.output,
            device.default_input,
            device.default_output
        );
    }
    Ok(())
}
