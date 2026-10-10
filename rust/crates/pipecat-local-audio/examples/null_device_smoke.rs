//! Native ALSA null-device smoke only: zero generation and sample discard.
//! This deliberately never selects a physical microphone or speaker.
use pipecat_audio::PcmFormat;
use pipecat_core::{AudioRawFrame, FrameData, Pipeline, PipelineConfig, WorkerState};
use pipecat_local_audio::{
    list_devices, AudioLimits, DeviceSelector, LocalAudioError, LocalAudioInput, LocalAudioOutput,
    LocalAudioTransport, LocalAudioTransportParams,
};
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let device = list_devices()?
        .into_iter()
        .find(|device| device.id == "alsa:null")
        .ok_or("ALSA null device unavailable; no native smoke was performed")?;
    println!("Native null-device smoke: {} [{}]", device.name, device.id);
    let baseline_threads = native_thread_count();
    let format = PcmFormat::new(16_000, 1)?;
    let limits = AudioLimits {
        queue_capacity: 4096,
        queue_byte_capacity: 8 * 1024 * 1024,
        ..Default::default()
    };
    let input = LocalAudioInput::new(DeviceSelector::Id(device.id.clone()), limits.clone())?;
    input.open(format)?;
    let mut capture = input.capture_stream()?;
    let frame = tokio::time::timeout(Duration::from_secs(2), capture.next_frame()).await??;
    let FrameData::InputAudio(audio) = frame.data else {
        return Err("expected captured PCM".into());
    };
    assert_eq!(audio.num_frames(), 320);
    assert!(audio.audio.iter().all(|byte| *byte == 0));
    drop(capture);
    assert!(!input.is_open());
    input.close();
    // Reopening after capture-owner destruction must not retain a native stream.
    input.open(format)?;
    input.close();
    println!("Captured one 20-ms zero chunk; capture-owner drop and reopen succeeded");

    input.open(format)?;
    let mut old_capture = input.capture_stream()?;
    input.close();
    assert_eq!(
        old_capture.next_frame().await,
        Err(LocalAudioError::Stopped)
    );
    input.open(format)?;
    let mut new_capture = input.capture_stream()?;
    drop(old_capture);
    assert!(input.is_open());
    let frame = tokio::time::timeout(Duration::from_secs(2), new_capture.next_frame()).await??;
    assert!(matches!(frame.data, FrameData::InputAudio(_)));
    drop(new_capture);
    input.close();
    println!("Dropping an old capture owner did not close the reopened session");

    let mut params = LocalAudioTransportParams {
        input_device: DeviceSelector::Id(device.id.clone()),
        output_device: DeviceSelector::Id(device.id.clone()),
        ..Default::default()
    };
    params.input.sample_rate = Some(16_000);
    params.input.stream_on_start = false;
    let transport = LocalAudioTransport::new(params)?;
    let worker = Pipeline::new(vec![transport.input()])
        .start(PipelineConfig::default())
        .await?;
    worker.end().await?;
    let (_, report) = worker.collect().await?;
    assert_eq!(report.state, WorkerState::Ended);
    assert!(!transport.input_backend().is_open());
    println!("Native input processor setup and graceful pipeline End released the stream");

    for cancelled in [false, true] {
        let mut params = LocalAudioTransportParams {
            input_device: DeviceSelector::Id(device.id.clone()),
            limits: limits.clone(),
            ..Default::default()
        };
        params.input.sample_rate = Some(16_000);
        params.input.queue_capacity = 4096;
        params.input.queue_byte_capacity = 8 * 1024 * 1024;
        let transport = LocalAudioTransport::new(params)?;
        let mut worker = Pipeline::new(vec![transport.input()])
            .start(PipelineConfig::default())
            .await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = worker.recv().await.ok_or("pipeline ended before capture")?;
                if matches!(frame.frame.data, FrameData::InputAudio(_)) {
                    return Ok::<(), Box<dyn std::error::Error>>(());
                }
            }
        })
        .await??;
        if cancelled {
            worker.cancel("null-device lifecycle smoke").await?;
        } else {
            worker.end().await?;
        }
        let (_, report) = worker.collect().await?;
        assert_eq!(
            report.state,
            if cancelled {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert!(!transport.input_backend().is_open());
    }
    println!("Actively capturing pipelines released their streams on End and Cancel");

    let output = LocalAudioOutput::new(
        DeviceSelector::Id(device.id.clone()),
        AudioLimits::default(),
    )?;
    output.open(format)?;
    let audio = AudioRawFrame::new(vec![0; 640], 16_000, 1)?;
    let first = output.write_audio(&audio).await?;
    assert_eq!(first.frames, 320);
    output.interrupt()?;
    let second = output.write_audio(&audio).await?;
    assert_eq!(second.frames, 320);
    output.close();
    output.close();
    assert!(!output.is_open());
    println!("Two 20-ms callback submissions, interruption/reopen, and idempotent close succeeded");
    concurrent_output_lifecycle(&device.id, format)?;
    println!("Concurrent native writers, interruption, and close/reopen released all streams");
    if let (Some(before), Some(after)) = (baseline_threads, native_thread_count()) {
        assert_eq!(
            before, after,
            "native callback threads were retained after close"
        );
        println!("Native thread count returned to baseline: {after}");
    }
    println!("No acoustic playback or physical microphone/speaker acceptance was tested");
    Ok(())
}

fn concurrent_output_lifecycle(
    id: &str,
    format: PcmFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Arc::new(LocalAudioOutput::new(
        DeviceSelector::Id(id.into()),
        AudioLimits::default(),
    )?);
    output.open(format)?;
    let start = Arc::new(Barrier::new(3));
    std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..2 {
            let output = output.clone();
            let start = start.clone();
            threads.push(scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                let audio = AudioRawFrame::new(vec![0; 32_000], 16_000, 1).unwrap();
                start.wait();
                for _ in 0..32 {
                    match runtime.block_on(output.write_audio(&audio)) {
                        Ok(receipt) => assert_eq!(receipt.frames, 16_000),
                        Err(
                            LocalAudioError::Stopped
                            | LocalAudioError::Cancelled
                            | LocalAudioError::QueueFull,
                        ) => {}
                        other => panic!("unexpected native writer result: {other:?}"),
                    }
                    std::thread::yield_now();
                }
            }));
        }
        let output = output.clone();
        threads.push(scope.spawn(move || {
            start.wait();
            for _ in 0..16 {
                output.interrupt().unwrap();
                output.close();
                output.open(format).unwrap();
            }
        }));
        for thread in threads {
            thread.join().unwrap();
        }
    });
    output.close();
    assert!(!output.is_open());
    Ok(())
}

fn native_thread_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_dir("/proc/self/task")
            .ok()
            .map(|entries| entries.count())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
