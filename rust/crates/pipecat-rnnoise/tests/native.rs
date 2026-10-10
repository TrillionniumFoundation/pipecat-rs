#![cfg(feature = "native")]
use pipecat_audio::{decode_pcm16_le, PcmFormat};
use pipecat_core::FrameData;
use pipecat_media_transport::AudioFilter;
use pipecat_rnnoise::{RateConversion, RnNoiseConfig, RnNoiseFilter};
use std::path::PathBuf;
fn config() -> RnNoiseConfig {
    RnNoiseConfig::new(
        std::env::var("PIPECAT_RNNOISE_LIBRARY").expect("explicit trusted library required"),
        std::env::var("PIPECAT_RNNOISE_SHA256").expect("explicit library identity required"),
    )
}
fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(
        std::env::var("PIPECAT_RNNOISE_FIXTURES").expect("C reference fixtures required"),
    );
    std::fs::read(path.join(name)).unwrap()
}
async fn process(config: RnNoiseConfig, input: &[u8], rate: u32, partition: usize) -> Vec<u8> {
    let filter = RnNoiseFilter::new(config).unwrap();
    filter
        .start(PcmFormat::new(rate, 1).unwrap())
        .await
        .unwrap();
    let mut output = Vec::new();
    for chunk in input.chunks(partition) {
        output.extend(filter.filter(chunk.to_vec()).await.unwrap());
    }
    filter.stop().await.unwrap();
    output
}
#[tokio::test]
#[ignore = "requires explicit pinned library and direct C reference; run qualification/run.sh"]
async fn native_matches_same_artifact_c_reference_across_partitions_and_reduces_noise() {
    let input = fixture("synthetic-noisy.pcm");
    let reference = fixture("reference-denoised.pcm");
    assert_eq!(input.len(), 288_000);
    assert_eq!(reference.len(), input.len());
    for partition in [137, 959, 960, 5001, 96_000] {
        assert_eq!(
            process(config(), &input, 48_000, partition).await,
            reference,
            "partition {partition}"
        );
    }
    let original = decode_pcm16_le(&input).unwrap();
    let denoised = decode_pcm16_le(&reference).unwrap();
    let energy = |values: &[i16]| {
        values
            .iter()
            .map(|&value| f64::from(value).powi(2))
            .sum::<f64>()
            / values.len() as f64
    };
    // A half-second after the final synthetic harmonic burst has ended.
    let noise_before = energy(&original[120_000..]);
    let noise_after = energy(&denoised[120_000..]);
    let speech_energy = energy(&denoised[26_400..96_000]);
    eprintln!("synthetic noise power before={noise_before:.3} after={noise_after:.3} ratio={:.6}; burst output power={speech_energy:.3}", noise_after/noise_before);
    assert!(
        noise_after < noise_before * 0.5,
        "real engine must attenuate noise-only region"
    );
    assert!(
        speech_energy > 10_000.0,
        "reject all-zero or near-silent fake implementation"
    );
    assert_ne!(reference, input);
}
#[tokio::test]
#[ignore = "requires explicit pinned native library"]
async fn native_restart_toggle_and_initialization_identity_are_explicit() {
    let filter = RnNoiseFilter::new(config()).unwrap();
    let format = PcmFormat::new(48_000, 1).unwrap();
    let input = fixture("synthetic-noisy.pcm");
    filter.start(format).await.unwrap();
    let expected = filter.filter(input[..9600].to_vec()).await.unwrap();
    filter.filter(input[9600..9619].to_vec()).await.unwrap();
    filter.stop().await.unwrap();
    filter.start(format).await.unwrap();
    assert_eq!(
        filter.filter(input[..9600].to_vec()).await.unwrap(),
        expected
    );
    filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap();
    assert_eq!(
        filter.filter(input[..777].to_vec()).await.unwrap(),
        input[..777]
    );
    let mut wrong = config();
    wrong.library_sha256 = "0".repeat(64);
    let wrong = RnNoiseFilter::new(wrong).unwrap();
    assert!(wrong
        .start(format)
        .await
        .unwrap_err()
        .message
        .contains("SHA-256 mismatch"));
    assert!(wrong.filter(input[..960].to_vec()).await.is_err());
    let mut absent = config();
    absent.library_path = absent.library_path.with_file_name("missing-rnnoise.so");
    assert!(RnNoiseFilter::new(absent)
        .unwrap()
        .start(format)
        .await
        .is_err());
}
#[tokio::test]
#[ignore = "requires explicit pinned native library"]
async fn native_sinc_round_trip_has_consistent_chunking_without_soxr_parity_claim() {
    let input = fixture("synthetic-noisy.pcm");
    for rate in [16_000, 24_000, 44_100] {
        let input = pipecat_audio::resample_pcm16(&input, 48_000, rate, 1).unwrap();
        let mut cfg = config();
        cfg.rate_conversion = RateConversion::NativeSinc;
        let bulk = process(cfg.clone(), &input, rate, 96_000).await;
        assert_eq!(process(cfg, &input, rate, 637).await, bulk);
        assert!(bulk.len() > input.len() * 95 / 100 && bulk.len() <= input.len());
    }
}
#[tokio::test]
#[ignore = "requires explicit pinned native library and direct C reference"]
async fn native_filter_runs_in_real_input_transport_and_discards_terminal_remainder() {
    use async_trait::async_trait;
    use pipecat_core::{AudioRawFrame, Direction, Frame, Pipeline, PipelineConfig, ProcessorError};
    use pipecat_media_transport::{
        InputBackend, InputIngress, InputTransport, InputTransportConfig,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Capture(AtomicUsize);
    #[async_trait]
    impl InputBackend for Capture {
        async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
            Ok(())
        }
        async fn capture(&self, _: InputIngress) -> Result<(), ProcessorError> {
            std::future::pending().await
        }
        async fn stop(&self) -> Result<(), ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let filter = Arc::new(RnNoiseFilter::new(config()).unwrap());
    let capture = Arc::new(Capture(AtomicUsize::new(0)));
    let transport = Arc::new(
        InputTransport::new(
            "rnnoise-qualification",
            InputTransportConfig {
                sample_rate: Some(48_000),
                stream_on_start: false,
                ..Default::default()
            },
            capture.clone(),
            Some(filter.clone()),
        )
        .unwrap(),
    );
    let ingress = transport.ingress();
    let worker = Pipeline::new(vec![transport])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !ingress.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut input = fixture("synthetic-noisy.pcm");
    input.extend([0; 318]);
    for chunk in input.chunks(9600) {
        let frame = Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(chunk.to_vec(), 48_000, 1).unwrap(),
        ));
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (frames, report) =
        tokio::time::timeout(std::time::Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let output: Vec<u8> = frames
        .into_iter()
        .filter_map(|frame| match frame.frame.data {
            FrameData::InputAudio(audio) => Some(audio.audio),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(output, fixture("reference-denoised.pcm"));
    assert_eq!(capture.0.load(Ordering::SeqCst), 1);
    assert!(!ingress.is_ready());
    assert!(filter.filter(vec![0; 960]).await.is_err());
}
#[tokio::test]
#[ignore = "requires explicitly compiled synthetic ABI failure fixtures"]
async fn synthetic_abi_failures_are_errors_not_fallback_audio() {
    use sha2::{Digest, Sha256};
    let directory = PathBuf::from(std::env::var("PIPECAT_RNNOISE_FIXTURES").unwrap());
    for (name, expected) in [
        ("wrong-frame", "480-sample"),
        ("null-state", "returned NULL"),
        ("missing-symbol", "rnnoise_destroy"),
    ] {
        let path = directory.join(format!("{name}.so"));
        let identity = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
        let filter = RnNoiseFilter::new(RnNoiseConfig::new(path, identity)).unwrap();
        assert!(
            filter
                .start(PcmFormat::new(48_000, 1).unwrap())
                .await
                .unwrap_err()
                .message
                .contains(expected),
            "fixture {name}"
        );
        assert!(filter.filter(vec![0; 960]).await.is_err());
    }
    let path = directory.join("nonfinite.so");
    let identity = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
    let filter = RnNoiseFilter::new(RnNoiseConfig::new(path, identity)).unwrap();
    filter
        .start(PcmFormat::new(48_000, 1).unwrap())
        .await
        .unwrap();
    assert!(filter
        .filter(vec![0; 960])
        .await
        .unwrap_err()
        .message
        .contains("non-finite"));
    assert!(filter
        .filter(vec![0; 960])
        .await
        .unwrap_err()
        .message
        .contains("not started"));
}
