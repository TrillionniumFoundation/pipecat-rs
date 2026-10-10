use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, pcm_to_wav, PcmFormat};
use pipecat_core::FrameData;
use pipecat_media_transport::mixer::{SoundLoadStatus, SoundfileMixer, SoundfileMixerConfig};
use pipecat_media_transport::AudioMixer;
use serde_json::json;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static COUNT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pipecat-mixer-{}-{}",
            std::process::id(),
            COUNT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, samples: &[i16], rate: u32, channels: u16) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(
            &path,
            pcm_to_wav(&encode_pcm16_le(samples), rate, channels).unwrap(),
        )
        .unwrap();
        path
    }
    fn config(&self, samples: &[i16]) -> SoundfileMixerConfig {
        SoundfileMixerConfig {
            sound_files: BTreeMap::from([("a".into(), self.write("a.wav", samples, 16000, 1))]),
            default_sound: "a".into(),
            volume: 1.0,
            ..Default::default()
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn start(config: SoundfileMixerConfig) -> SoundfileMixer {
    let mixer = SoundfileMixer::new(config).unwrap();
    mixer
        .start(PcmFormat::new(16000, 1).unwrap())
        .await
        .unwrap();
    mixer
}
async fn mix(mixer: &SoundfileMixer, samples: &[i16]) -> Vec<i16> {
    decode_pcm16_le(&mixer.mix(encode_pcm16_le(samples)).await.unwrap()).unwrap()
}
fn settings(value: serde_json::Value) -> FrameData {
    FrameData::MixerUpdateSettings(serde_json::from_value(value).unwrap())
}

#[tokio::test]
async fn loads_matching_pcm16_wav_and_consumes_sequentially() {
    let fixture = Fixture::new();
    let mixer = start(fixture.config(&[1, 2, 3, 4])).await;
    assert_eq!(
        mixer.load_statuses().unwrap()["a"],
        SoundLoadStatus::Loaded { samples: 4 }
    );
    assert_eq!(mix(&mixer, &[0, 0]).await, [1, 2]);
    assert_eq!(mix(&mixer, &[0, 0]).await, [3, 4]);
}

#[tokio::test]
async fn scales_clips_and_truncates_only_after_adding() {
    let fixture = Fixture::new();
    let mixer = start(fixture.config(&[30000, -30000, 3, -3])).await;
    assert_eq!(mix(&mixer, &[30000, -30000]).await, [32767, -32768]);
    mixer
        .control(&settings(json!({"volume":0.5})))
        .await
        .unwrap();
    assert_eq!(mix(&mixer, &[-1, 1]).await, [0, 0]);
}

#[tokio::test]
async fn whole_chunk_loop_restarts_and_oversized_chunks_keep_output_length() {
    let fixture = Fixture::new();
    let mixer = start(fixture.config(&[1, 2, 3, 4])).await;
    assert_eq!(mix(&mixer, &[0, 0, 0]).await, [1, 2, 3]);
    assert_eq!(mix(&mixer, &[0, 0, 0]).await, [1, 2, 3]);
    assert_eq!(mix(&mixer, &[0; 10]).await, [1, 2, 3, 4, 1, 2, 3, 4, 1, 2]);
}

#[tokio::test]
async fn non_looping_incomplete_chunk_passes_through_without_advancing() {
    let fixture = Fixture::new();
    let mut config = fixture.config(&[1, 2, 3, 4]);
    config.loop_audio = false;
    let mixer = start(config).await;
    assert_eq!(mix(&mixer, &[0, 0, 0]).await, [1, 2, 3]);
    assert_eq!(mix(&mixer, &[5, 5, 5]).await, [5, 5, 5]);
    assert_eq!(mixer.position().unwrap(), 3);
    assert_eq!(mix(&mixer, &[0]).await, [4]);
}

#[tokio::test]
async fn enable_settings_and_sound_selection_preserve_or_reset_position() {
    let fixture = Fixture::new();
    let mut config = fixture.config(&[100, 200, 300, 400]);
    config
        .sound_files
        .insert("b".into(), fixture.write("b.wav", &[5, 6, 7, 8], 16000, 1));
    let mixer = start(config).await;
    assert_eq!(mix(&mixer, &[0, 0]).await, [100, 200]);
    mixer.control(&FrameData::MixerEnable(false)).await.unwrap();
    assert_eq!(mix(&mixer, &[10, 10]).await, [10, 10]);
    assert_eq!(mixer.position().unwrap(), 2);
    mixer.control(&FrameData::MixerEnable(true)).await.unwrap();
    mixer
        .control(&settings(json!({"sound":"b", "volume":2, "loop":false})))
        .await
        .unwrap();
    assert_eq!(mix(&mixer, &[0, 0]).await, [10, 12]);
    assert_eq!(mixer.current_sound().unwrap(), "b");
}

#[tokio::test]
async fn unknown_or_malformed_settings_are_explicit_and_atomic() {
    let fixture = Fixture::new();
    let mixer = start(fixture.config(&[10, 20])).await;
    for update in [
        json!({"sound":"missing", "volume":0}),
        json!({"sound":"a", "volume":"bad"}),
        json!({"loop":3}),
    ] {
        assert!(mixer.control(&settings(update)).await.is_err());
    }
    assert_eq!(mix(&mixer, &[0, 0]).await, [10, 20]);
}

#[tokio::test]
async fn invalid_missing_empty_rate_and_channel_files_have_explicit_statuses() {
    let fixture = Fixture::new();
    let mut config = fixture.config(&[1]);
    config.default_sound = "missing".into();
    config.sound_files.extend([
        ("missing".into(), fixture.0.join("missing.wav")),
        ("rate".into(), fixture.write("rate.wav", &[1], 8000, 1)),
        (
            "stereo".into(),
            fixture.write("stereo.wav", &[1, 2], 16000, 2),
        ),
        ("empty".into(), fixture.write("empty.wav", &[], 16000, 1)),
    ]);
    let invalid = fixture.0.join("invalid.wav");
    std::fs::write(&invalid, b"not a wav").unwrap();
    config.sound_files.insert("invalid".into(), invalid);
    let mixer = start(config).await;
    let statuses = mixer.load_statuses().unwrap();
    assert!(matches!(statuses["a"], SoundLoadStatus::Loaded { .. }));
    for name in ["missing", "rate", "stereo", "empty", "invalid"] {
        assert!(matches!(&statuses[name], SoundLoadStatus::Failed(reason) if !reason.is_empty()));
    }
    assert_eq!(mix(&mixer, &[3, 4]).await, [3, 4]);
}

#[tokio::test]
async fn limits_and_bad_pcm_fail_without_panics() {
    let fixture = Fixture::new();
    let mut config = fixture.config(&[1, 2]);
    config.max_sound_bytes = 44;
    let mixer = start(config).await;
    assert!(matches!(
        mixer.load_statuses().unwrap()["a"],
        SoundLoadStatus::Failed(_)
    ));
    assert!(mixer.mix(vec![1]).await.is_err());
    assert!(mixer
        .start(PcmFormat::new(16000, 2).unwrap())
        .await
        .is_err());
    let mut config = fixture.config(&[1]);
    config.volume = f64::NAN;
    assert!(SoundfileMixer::new(config).is_err());
}

#[tokio::test]
async fn stop_is_idempotent_and_restart_reloads_from_beginning() {
    let fixture = Fixture::new();
    let mixer = start(fixture.config(&[1, 2, 3])).await;
    assert_eq!(mix(&mixer, &[0]).await, [1]);
    mixer.stop().await.unwrap();
    mixer.stop().await.unwrap();
    assert_eq!(mix(&mixer, &[10]).await, [10]);
    mixer
        .start(PcmFormat::new(16000, 1).unwrap())
        .await
        .unwrap();
    assert_eq!(mix(&mixer, &[0]).await, [1]);
}
