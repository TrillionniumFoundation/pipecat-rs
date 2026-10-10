//! Explicit local qualification; never downloads assets.
#![cfg(feature = "espeak-gpl")]
use pipecat_piper::{sha256_bytes, PiperConfig, PiperError, PiperService, PiperSettings};
use std::time::Duration;
fn config() -> PiperConfig {
    let manifest = std::env::var_os("PIPECAT_PIPER_MANIFEST")
        .expect("set PIPECAT_PIPER_MANIFEST to an explicit local JSON config");
    serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap()
}
fn check(pcm: &[u8]) {
    assert!(!pcm.is_empty() && pcm.len().is_multiple_of(2));
    let energy: f64 = pcm
        .as_chunks::<2>()
        .0
        .iter()
        .map(|s| {
            let v = f64::from(i16::from_le_bytes([s[0], s[1]]));
            v * v
        })
        .sum();
    assert!(
        (energy / (pcm.len() / 2) as f64).sqrt() > 50.,
        "speech output must not be silence"
    );
}
#[tokio::test]
#[ignore = "requires explicit hashed local Piper model, ONNX runtime, and GPL eSpeak artifacts"]
async fn genuine_cpu_inference_cancellation_resampling_and_global_owner() {
    let mut config = config();
    config.queue_capacity = 1;
    let service = PiperService::load(config.clone()).await.unwrap();
    let one = service
        .submit(
            "The quick brown fox jumps over the lazy dog.",
            "first",
            PiperSettings::default(),
        )
        .unwrap();
    assert!(matches!(
        service.submit("full", "second", PiperSettings::default()),
        Err(PiperError::Backpressure)
    ));
    let one = one.result().await.unwrap();
    check(&one.pcm);
    assert_eq!(one.context_id, "first");
    assert_eq!(
        one.sample_rate,
        config
            .output_sample_rate
            .unwrap_or(service.voice_config().audio.sample_rate)
    );
    let two = service
        .submit(
            "Today is a beautiful day for a walk in the park.",
            "second",
            PiperSettings::default(),
        )
        .unwrap()
        .result()
        .await
        .unwrap();
    check(&two.pcm);
    assert_ne!(one.pcm, two.pcm);
    let long = service
        .submit(
            "We are testing native model cancellation. ".repeat(70),
            "long",
            PiperSettings::default(),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    long.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(10), long.result())
            .await
            .unwrap(),
        Err(PiperError::Cancelled)
    ));
    let fresh = service
        .submit(
            "Cancellation did not break the next request.",
            "fresh",
            PiperSettings::default(),
        )
        .unwrap()
        .result()
        .await
        .unwrap();
    check(&fresh.pcm);
    let old = service
        .submit(
            "This result belongs to the old context.",
            "old",
            PiperSettings::default(),
        )
        .unwrap();
    service.interrupt();
    assert!(matches!(old.result().await, Err(PiperError::Cancelled)));
    // Multiple model owners share one serialized process-global phonemizer.
    let mut other_config = config.clone();
    other_config.output_sample_rate = None;
    let other = PiperService::load(other_config).await.unwrap();
    let left = service
        .submit(
            "The first voice owner is speaking.",
            "left",
            PiperSettings::default(),
        )
        .unwrap();
    let right = other
        .submit(
            "The second voice owner is speaking.",
            "right",
            PiperSettings::default(),
        )
        .unwrap();
    let (left, right) = tokio::join!(left.result(), right.result());
    check(&left.unwrap().pcm);
    let right = right.unwrap();
    check(&right.pcm);
    assert_eq!(right.sample_rate, other.voice_config().audio.sample_rate);
    other.drain().await.unwrap();
    service.shutdown().await.unwrap();
    assert!(!service.is_running());
    assert!(matches!(
        service.submit("closed", "x", PiperSettings::default()),
        Err(PiperError::Closed)
    ));
}
#[tokio::test]
#[ignore = "requires explicit hashed local native assets"]
async fn malformed_model_config_and_output_bounds_fail_explicitly() {
    let config = config();
    let unique = format!("pipecat-piper-native-{}", std::process::id());
    let root = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&root).unwrap();
    let bad_path = root.join("bad.onnx");
    std::fs::write(&bad_path, b"not an ONNX model").unwrap();
    let mut bad = config.clone();
    bad.model = pipecat_piper::Artifact::new(&bad_path, sha256_bytes(b"not an ONNX model"));
    assert!(matches!(
        PiperService::load(bad).await,
        Err(PiperError::Model(_))
    ));
    let bytes = std::fs::read(&config.config.path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["num_speakers"] = serde_json::json!(2);
    let bytes = serde_json::to_vec(&value).unwrap();
    let path = root.join("bad.json");
    std::fs::write(&path, &bytes).unwrap();
    let mut bad = config.clone();
    bad.config = pipecat_piper::Artifact::new(&path, sha256_bytes(&bytes));
    assert!(matches!(
        PiperService::load(bad).await,
        Err(PiperError::Model(_))
    ));
    let mut tiny = config.clone();
    tiny.max_audio_samples = 1;
    let service = PiperService::load(tiny).await.unwrap();
    assert!(matches!(
        service
            .submit(
                "Too much speech for one sample.",
                "limit",
                PiperSettings::default()
            )
            .unwrap()
            .result()
            .await,
        Err(PiperError::TooLarge)
    ));
    service.shutdown().await.unwrap();
    let mut bad = config.clone();
    let espeak = bad.espeak.as_mut().unwrap();
    let copied = root.join("copied-espeak.so");
    std::fs::copy(&espeak.library.path, &copied).unwrap();
    espeak.library.path = copied;
    assert!(
        matches!(PiperService::load(bad).await, Err(PiperError::Invalid(_))),
        "global owner must reject a different library identity"
    );
    std::fs::remove_dir_all(root).unwrap();
}
