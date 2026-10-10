//! Independent contract checks. Native cases use only an explicitly supplied local runtime.
use pipecat_piper::{sha256_directory, PiperError, VoiceConfig};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
struct TempDir(PathBuf);
impl TempDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-piper-review-{label}-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn voice_value() -> serde_json::Value {
    serde_json::json!({
        "audio": {"sample_rate": 22050}, "phoneme_type": "text",
        "espeak": {"voice": "en-us"}, "num_symbols": 6, "num_speakers": 1,
        "inference": {"noise_scale": 0.667, "length_scale": 1.0, "noise_w": 0.8},
        "phoneme_id_map": {"_": [0], "^": [1], "$": [2], "a": [3], "e": [4], "́": [5]}
    })
}

#[test]
fn source_config_defaults_accept_absent_and_partial_inference() {
    for inference in [
        None,
        Some(serde_json::json!({})),
        Some(serde_json::json!({"noise_w": 0.4})),
    ] {
        let mut value = voice_value();
        value.as_object_mut().unwrap().remove("phoneme_type");
        if let Some(inference) = inference {
            value["inference"] = inference;
        } else {
            value.as_object_mut().unwrap().remove("inference");
        }
        let parsed = VoiceConfig::parse(&serde_json::to_vec(&value).unwrap());
        assert!(
            parsed.is_ok(),
            "Piper 1.3 defaults must accept this config: {parsed:?}"
        );
        let parsed = parsed.unwrap();
        assert_eq!(parsed.phoneme_type, "espeak");
        assert_eq!(parsed.inference.length_scale, 1.0);
        assert_eq!(parsed.inference.noise_scale, 0.667);
        assert_eq!(
            parsed.inference.noise_w,
            value["inference"]["noise_w"]
                .as_f64()
                .map_or(0.8, |v| v as f32)
        );
    }
}

#[test]
fn remapped_ids_preserve_bos_pad_eos_and_enforce_expanded_size() {
    let mut value = voice_value();
    value["phoneme_map"] = serde_json::json!({"é": ["e", "́"]});
    value["phoneme_id_map"]["e"] = serde_json::json!([4, 3]);
    let voice = VoiceConfig::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(
        voice.phoneme_ids(&['é'], 8).unwrap(),
        [1, 0, 4, 3, 0, 5, 0, 2]
    );
    assert!(matches!(
        voice.phoneme_ids(&['é'], 7),
        Err(PiperError::TooLarge)
    ));
    assert!(matches!(
        voice.phoneme_ids(&['x'], 20),
        Err(PiperError::UnknownPhoneme('x'))
    ));
}

#[cfg(unix)]
#[test]
fn directory_fingerprint_cannot_hide_backslash_name_behind_real_subdirectory() {
    let root = TempDir::new("names");
    std::fs::create_dir_all(root.0.join("a")).unwrap();
    std::fs::write(root.0.join("a/b"), b"real").unwrap();
    std::fs::write(root.0.join("a\\b"), b"hidden").unwrap();
    assert!(matches!(
        sha256_directory(&root.0),
        Err(PiperError::Invalid(_))
    ));
}

#[test]
fn directory_size_limit_rejects_sparse_files_before_hashing() {
    let root = TempDir::new("size");
    let file = std::fs::File::create(root.0.join("oversize")).unwrap();
    file.set_len(128 * 1024 * 1024 + 1).unwrap();
    assert!(matches!(
        sha256_directory(&root.0),
        Err(PiperError::TooLarge)
    ));
}

#[cfg(feature = "onnx")]
mod native {
    use super::*;
    use pipecat_piper::{sha256_bytes, Artifact, PiperConfig, PiperService, PiperSettings};
    use std::{future::Future, task::Poll, time::Duration};

    fn manifest() -> PiperConfig {
        let path =
            std::env::var_os("PIPECAT_PIPER_MANIFEST").expect("explicit local manifest required");
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    // Small test-owned ONNX protobuf, built in Rust. This is an invariant fixture,
    // not a speech model or evidence of genuine neural speech synthesis.
    fn varint(mut n: u64, out: &mut Vec<u8>) {
        while n >= 128 {
            out.push((n as u8 & 127) | 128);
            n >>= 7;
        }
        out.push(n as u8);
    }
    fn number(field: u64, n: u64, out: &mut Vec<u8>) {
        varint(field << 3, out);
        varint(n, out);
    }
    fn bytes(field: u64, b: &[u8], out: &mut Vec<u8>) {
        varint((field << 3) | 2, out);
        varint(b.len() as u64, out);
        out.extend_from_slice(b);
    }
    fn value_info(name: &str, elem_type: u64, dims: &[i64]) -> Vec<u8> {
        let mut shape = Vec::new();
        for dim in dims {
            let mut encoded = Vec::new();
            if *dim < 0 {
                bytes(2, b"sequence", &mut encoded);
            } else {
                number(1, *dim as u64, &mut encoded);
            }
            bytes(1, &encoded, &mut shape);
        }
        let mut tensor = Vec::new();
        number(1, elem_type, &mut tensor);
        bytes(2, &shape, &mut tensor);
        let mut ty = Vec::new();
        bytes(1, &tensor, &mut ty);
        let mut value = Vec::new();
        bytes(1, name.as_bytes(), &mut value);
        bytes(2, &ty, &mut value);
        value
    }
    fn constant_model(samples: &[f32]) -> Vec<u8> {
        let mut tensor = Vec::new();
        for dim in [1, 1, samples.len() as u64] {
            number(1, dim, &mut tensor);
        }
        number(2, 1, &mut tensor);
        bytes(8, b"output", &mut tensor);
        let raw: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
        bytes(9, &raw, &mut tensor);
        let mut graph = Vec::new();
        bytes(2, b"piper_review_constant", &mut graph);
        bytes(5, &tensor, &mut graph);
        for (name, ty, dims) in [
            ("input", 7, vec![1, -1]),
            ("input_lengths", 7, vec![1]),
            ("scales", 1, vec![3]),
        ] {
            bytes(11, &value_info(name, ty, &dims), &mut graph);
        }
        bytes(
            12,
            &value_info("output", 1, &[1, 1, samples.len() as i64]),
            &mut graph,
        );
        let mut model = Vec::new();
        number(1, 8, &mut model);
        bytes(7, &graph, &mut model);
        let mut opset = Vec::new();
        number(2, 13, &mut opset);
        bytes(8, &opset, &mut model);
        model
    }
    fn probe_model(source: &str) -> Vec<u8> {
        let mut graph = Vec::new();
        bytes(2, b"piper_review_input_probe", &mut graph);
        let mut inputs = vec![
            ("input", 7, vec![1, -1]),
            ("input_lengths", 7, vec![1]),
            ("scales", 1, vec![3]),
        ];
        if source == "sid" {
            inputs.push(("sid", 7, vec![1]));
        }
        for (name, ty, dims) in inputs {
            bytes(11, &value_info(name, ty, &dims), &mut graph);
        }
        let mut cast = Vec::new();
        bytes(1, source.as_bytes(), &mut cast);
        bytes(2, b"cast_value", &mut cast);
        bytes(4, b"Cast", &mut cast);
        let mut attribute = Vec::new();
        bytes(1, b"to", &mut attribute);
        number(3, 1, &mut attribute);
        number(20, 2, &mut attribute);
        bytes(5, &attribute, &mut cast);
        bytes(1, &cast, &mut graph);
        let mut shape = Vec::new();
        number(1, 3, &mut shape);
        number(2, 7, &mut shape);
        bytes(8, b"shape", &mut shape);
        let shape_data: Vec<_> = [1i64, 1, -1]
            .into_iter()
            .flat_map(i64::to_le_bytes)
            .collect();
        bytes(9, &shape_data, &mut shape);
        bytes(5, &shape, &mut graph);
        let mut reshape = Vec::new();
        bytes(1, b"cast_value", &mut reshape);
        bytes(1, b"shape", &mut reshape);
        bytes(2, b"output", &mut reshape);
        bytes(4, b"Reshape", &mut reshape);
        bytes(1, &reshape, &mut graph);
        bytes(12, &value_info("output", 1, &[1, 1, -1]), &mut graph);
        let mut model = Vec::new();
        number(1, 8, &mut model);
        bytes(7, &graph, &mut model);
        let mut opset = Vec::new();
        number(2, 13, &mut opset);
        bytes(8, &opset, &mut model);
        model
    }
    fn fixture_config(dir: &TempDir, label: &str, samples: &[f32]) -> PiperConfig {
        let mut config = manifest();
        let model = constant_model(samples);
        let voice = serde_json::to_vec(&voice_value()).unwrap();
        let model_path = dir.0.join(format!("{label}.onnx"));
        let config_path = dir.0.join(format!("{label}.json"));
        std::fs::write(&model_path, &model).unwrap();
        std::fs::write(&config_path, &voice).unwrap();
        config.model = Artifact::new(model_path, sha256_bytes(&model));
        config.config = Artifact::new(config_path, sha256_bytes(&voice));
        config.espeak = None;
        config.output_sample_rate = None;
        config
    }
    fn pcm_samples(pcm: &[u8]) -> Vec<i16> {
        assert!(pcm.len().is_multiple_of(2));
        pcm.as_chunks::<2>()
            .0
            .iter()
            .copied()
            .map(i16::from_le_bytes)
            .collect()
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn model_inputs_preserve_normalized_ids_scales_and_speaker_snapshots() {
        let root = TempDir::new("inputs");
        for source in ["input", "scales", "sid"] {
            let mut config = fixture_config(&root, source, &[0.]);
            let model = probe_model(source);
            std::fs::write(&config.model.path, &model).unwrap();
            config.model.sha256 = sha256_bytes(&model);
            if source == "sid" {
                let mut voice = voice_value();
                voice["num_speakers"] = serde_json::json!(3);
                let bytes = serde_json::to_vec(&voice).unwrap();
                std::fs::write(&config.config.path, &bytes).unwrap();
                config.config.sha256 = sha256_bytes(&bytes);
            }
            let service = PiperService::load(config).await.unwrap();
            let first = service
                .submit("é", "default", PiperSettings::default())
                .unwrap();
            let second = service
                .submit(
                    "é",
                    "custom",
                    PiperSettings {
                        speaker_id: (source == "sid").then_some(2),
                        noise_scale: Some(0.25),
                        length_scale: Some(2.0),
                        noise_w: Some(0.75),
                    },
                )
                .unwrap();
            let (default, custom) = tokio::join!(first.result(), second.result());
            let (expected_default, expected_custom): (&[i16], &[i16]) = match source {
                "input" => (
                    &[6553, 0, 26213, 0, 32767, 0, 13106],
                    &[6553, 0, 26213, 0, 32767, 0, 13106],
                ),
                "scales" => (&[21855, 32767, 26213], &[4095, 32767, 12287]),
                "sid" => (&[0], &[32767]),
                _ => unreachable!(),
            };
            assert_eq!(
                pcm_samples(&default.unwrap().pcm),
                expected_default,
                "default {source}"
            );
            assert_eq!(
                pcm_samples(&custom.unwrap().pcm),
                expected_custom,
                "custom {source}"
            );
            service.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn model_hash_mismatch_and_changed_runtime_identity_fail_closed() {
        let root = TempDir::new("identity");
        let config = fixture_config(&root, "good", &[0.5]);
        let service = PiperService::load(config.clone()).await.unwrap();
        service.shutdown().await.unwrap();
        let mut corrupt = config.clone();
        corrupt.model.sha256 = "0".repeat(64);
        assert!(matches!(
            PiperService::load(corrupt).await,
            Err(PiperError::HashMismatch)
        ));
        let mut other_runtime = config;
        let copied = root.0.join("same-runtime-other-path.so");
        std::fs::copy(&other_runtime.runtime.path, &copied).unwrap();
        other_runtime.runtime.path = copied;
        assert!(matches!(
            PiperService::load(other_runtime).await,
            Err(PiperError::Invalid(_))
        ));
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn completed_single_slot_request_can_be_followed_by_immediate_submission() {
        let root = TempDir::new("permit");
        let mut config = fixture_config(&root, "one", &[0.5]);
        config.queue_capacity = 1;
        let service = PiperService::load(config).await.unwrap();
        for index in 0..5000 {
            let pending = service
                .submit("a", "sequential", PiperSettings::default())
                .unwrap_or_else(|e| {
                    panic!(
                        "slot must be released before result is observable; iteration {index}: {e}"
                    )
                });
            pending.result().await.unwrap();
        }
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn quiet_audio_normalization_matches_piper_13_and_zero_guard() {
        let root = TempDir::new("normalize");
        for (label, samples, expected) in [
            ("quiet", vec![0.001, -0.0005, 0.], vec![32767i16, -16383, 0]),
            ("near_zero", vec![1e-9, -1e-9], vec![0, 0]),
        ] {
            let service = PiperService::load(fixture_config(&root, label, &samples))
                .await
                .unwrap();
            let result = service
                .submit("a", label, PiperSettings::default())
                .unwrap()
                .result()
                .await
                .unwrap();
            let got = pcm_samples(&result.pcm);
            assert_eq!(got, expected, "normalization fixture {label}");
            service.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn finite_nonempty_output_and_resampled_sample_bounds_are_enforced() {
        let root = TempDir::new("output");
        for (label, samples) in [
            ("nan", vec![f32::NAN]),
            ("inf", vec![f32::INFINITY]),
            ("empty", vec![]),
        ] {
            let service = PiperService::load(fixture_config(&root, label, &samples))
                .await
                .unwrap();
            assert!(
                matches!(
                    service
                        .submit("a", label, PiperSettings::default())
                        .unwrap()
                        .result()
                        .await,
                    Err(PiperError::Model(_))
                ),
                "fixture {label}"
            );
            service.shutdown().await.unwrap();
        }
        let mut config = fixture_config(&root, "upsample", &[0.5; 100]);
        config.output_sample_rate = Some(44100);
        config.max_audio_samples = 199;
        let service = PiperService::load(config).await.unwrap();
        assert!(matches!(
            service
                .submit("a", "too-many", PiperSettings::default())
                .unwrap()
                .result()
                .await,
            Err(PiperError::TooLarge)
        ));
        service.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires an explicit trusted local ONNX runtime manifest"]
    async fn dropped_drain_waiter_does_not_lose_join_or_accepted_result() {
        let root = TempDir::new("join");
        let mut config = fixture_config(&root, "small", &[0.5; 100]);
        config.queue_capacity = 1;
        let service = PiperService::load(config).await.unwrap();
        let pending = service
            .submit("a", "kept", PiperSettings::default())
            .unwrap();
        let mut drain = Box::pin(service.drain());
        std::future::poll_fn(|cx| {
            let _ = drain.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        drop(drain);
        let result = pending.result().await.unwrap();
        assert_eq!(result.context_id, "kept");
        tokio::time::timeout(Duration::from_secs(2), service.drain())
            .await
            .unwrap()
            .unwrap();
        assert!(!service.is_running());
        assert!(matches!(
            service.submit("a", "closed", PiperSettings::default()),
            Err(PiperError::Closed)
        ));
        drop(service);
    }

    #[cfg(feature = "espeak-gpl")]
    #[tokio::test]
    #[ignore = "requires explicit trusted local Piper model, eSpeak and runtime artifacts"]
    async fn native_cancel_drop_then_reload_preserves_global_owner() {
        let mut config = manifest();
        config.queue_capacity = 2;
        let service = PiperService::load(config.clone()).await.unwrap();
        let old = service
            .submit(
                "This request will be interrupted. ".repeat(60),
                "old",
                PiperSettings::default(),
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut shutdown = Box::pin(service.shutdown());
        std::future::poll_fn(|cx| {
            let _ = shutdown.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        drop(shutdown);
        drop(service);
        assert!(matches!(old.result().await, Err(PiperError::Cancelled)));
        let service = PiperService::load(config).await.unwrap();
        let fresh = service
            .submit(
                "The native engine is still available.",
                "fresh",
                PiperSettings::default(),
            )
            .unwrap()
            .result()
            .await
            .unwrap();
        assert!(fresh.pcm.len() > 16000);
        service.shutdown().await.unwrap();
    }
}
