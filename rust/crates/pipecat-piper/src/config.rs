use crate::{PiperError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};
#[cfg(any(feature = "onnx", test))]
use unicode_normalization::UnicodeNormalization;

/// Caller-owned, trusted local artifact. SHA-256 pins identity, not trustworthiness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub path: PathBuf,
    pub sha256: String,
}
impl Artifact {
    pub fn new(path: impl Into<PathBuf>, sha256: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            sha256: sha256.into(),
        }
    }
    #[cfg(any(feature = "onnx", test))]
    pub(crate) fn read(&self, max: usize) -> Result<Vec<u8>> {
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(PiperError::Invalid(
                "SHA-256 must contain 64 hexadecimal digits",
            ));
        }
        if !std::fs::metadata(&self.path)?.is_file() {
            return Err(PiperError::Invalid("artifact must be a regular file"));
        }
        let file = std::fs::File::open(&self.path)?;
        if !file.metadata()?.is_file() {
            return Err(PiperError::Invalid("artifact must be a regular file"));
        }
        let mut bytes = Vec::new();
        file.take(max as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > max {
            return Err(PiperError::TooLarge);
        }
        if sha256_bytes(&bytes) != self.sha256.to_ascii_lowercase() {
            return Err(PiperError::HashMismatch);
        }
        Ok(bytes)
    }
}
pub fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn sha256_file(path: impl AsRef<Path>) -> Result<String> {
    if !std::fs::metadata(path.as_ref())?.is_file() {
        return Err(PiperError::Invalid("artifact must be a regular file"));
    }
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
/// Stable data-directory fingerprint: sorted relative UTF-8 names, NUL, file SHA-256, LF.
/// Symlinks/special files are refused; at most 10000 entries, 32 directory levels, and 128 MiB total.
pub fn sha256_directory(path: impl AsRef<Path>) -> Result<String> {
    let root = path.as_ref();
    if !std::fs::symlink_metadata(root)?.file_type().is_dir() {
        return Err(PiperError::Invalid(
            "data root must be a directory, not a symlink",
        ));
    }
    let mut files = BTreeMap::new();
    let mut pending = vec![(root.to_path_buf(), 0usize)];
    let mut count = 0usize;
    let mut size = 0u64;
    while let Some((dir, depth)) = pending.pop() {
        if depth > 32 {
            return Err(PiperError::TooLarge);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            count += 1;
            if count > 10000 {
                return Err(PiperError::TooLarge);
            }
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push((entry.path(), depth + 1));
            } else if kind.is_file() {
                size = size
                    .checked_add(entry.metadata()?.len())
                    .ok_or(PiperError::TooLarge)?;
                if size > 128 * 1024 * 1024 {
                    return Err(PiperError::TooLarge);
                }
                let name = entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| PiperError::Invalid("data path"))?
                    .to_str()
                    .ok_or(PiperError::Invalid("data filenames must be UTF-8"))?
                    .to_string();
                if name.contains('\\') {
                    return Err(PiperError::Invalid(
                        "data filenames cannot contain backslashes",
                    ));
                }
                if files.insert(name, entry.path()).is_some() {
                    return Err(PiperError::Invalid("duplicate data filename"));
                }
            } else {
                return Err(PiperError::Invalid(
                    "phonemizer data cannot contain symlinks or special files",
                ));
            }
        }
    }
    let mut hash = Sha256::new();
    for (name, path) in files {
        hash.update(name.as_bytes());
        hash.update([0]);
        hash.update(sha256_file(path)?.as_bytes());
        hash.update(b"\n");
    }
    Ok(format!("{:x}", hash.finalize()))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EspeakConfig {
    pub library: Artifact,
    /// The espeak-ng-data directory, not its parent.
    pub data_directory: PathBuf,
    pub data_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PiperConfig {
    pub voice: String,
    pub model: Artifact,
    pub config: Artifact,
    pub runtime: Artifact,
    pub espeak: Option<EspeakConfig>,
    pub output_sample_rate: Option<u32>,
    pub inference_threads: usize,
    /// Includes queued/running work AND completed but uncollected results.
    pub queue_capacity: usize,
    pub max_text_bytes: usize,
    pub max_phonemes: usize,
    pub max_audio_samples: usize,
    pub max_model_bytes: usize,
    /// Deliberately rejected; no implicit accelerator fallback.
    pub use_cuda: bool,
    /// Deliberately rejected; this service never acquires models at runtime.
    pub download: bool,
}
impl PiperConfig {
    pub fn new(
        voice: impl Into<String>,
        model: Artifact,
        config: Artifact,
        runtime: Artifact,
    ) -> Self {
        Self {
            voice: voice.into(),
            model,
            config,
            runtime,
            espeak: None,
            output_sample_rate: None,
            inference_threads: 2,
            queue_capacity: 4,
            max_text_bytes: 4096,
            max_phonemes: 8192,
            max_audio_samples: 48_000 * 120,
            max_model_bytes: 500 * 1024 * 1024,
            use_cuda: false,
            download: false,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.voice.trim().is_empty() || self.voice.len() > 256 || self.voice.contains('\0') {
            return Err(PiperError::Invalid("Piper TTS voice must be specified"));
        }
        if self.use_cuda {
            return Err(PiperError::Unsupported("CUDA; only CPU is implemented"));
        }
        if self.download {
            return Err(PiperError::Unsupported("automatic model download"));
        }
        if !(1..=32).contains(&self.inference_threads)
            || !(1..=64).contains(&self.queue_capacity)
            || !(1..=65536).contains(&self.max_text_bytes)
            || !(4..=65536).contains(&self.max_phonemes)
            || !(1..=48_000 * 600).contains(&self.max_audio_samples)
            || !(1..=500 * 1024 * 1024).contains(&self.max_model_bytes)
            || self
                .output_sample_rate
                .is_some_and(|s| !(8000..=96000).contains(&s))
        {
            return Err(PiperError::Invalid(
                "invalid resource bounds, threads, queue, or sample rate",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PiperSettings {
    pub speaker_id: Option<i64>,
    pub length_scale: Option<f32>,
    pub noise_scale: Option<f32>,
    pub noise_w: Option<f32>,
}
impl PiperSettings {
    pub(crate) fn validate(&self, voice: &VoiceConfig) -> Result<()> {
        if self
            .speaker_id
            .is_some_and(|s| s < 0 || s >= voice.num_speakers)
            || self
                .length_scale
                .is_some_and(|s| !s.is_finite() || !(0.1..=4.0).contains(&s))
            || self
                .noise_scale
                .is_some_and(|s| !s.is_finite() || !(0.0..=2.0).contains(&s))
            || self
                .noise_w
                .is_some_and(|s| !s.is_finite() || !(0.0..=2.0).contains(&s))
        {
            return Err(PiperError::Invalid("invalid speaker or synthesis scales"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize)]
pub struct VoiceConfig {
    pub audio: AudioConfig,
    #[serde(default = "default_phoneme_type")]
    pub phoneme_type: String,
    pub espeak: Option<EspeakVoice>,
    pub num_symbols: i64,
    pub num_speakers: i64,
    #[serde(default)]
    pub inference: InferenceConfig,
    pub phoneme_id_map: BTreeMap<char, Vec<i64>>,
    #[serde(default)]
    pub phoneme_map: BTreeMap<char, Vec<char>>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct AudioConfig {
    pub sample_rate: u32,
}
#[derive(Clone, Debug, Deserialize)]
pub struct EspeakVoice {
    pub voice: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct InferenceConfig {
    pub noise_scale: f32,
    pub length_scale: f32,
    pub noise_w: f32,
}
fn default_phoneme_type() -> String {
    "espeak".into()
}
impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            noise_scale: 0.667,
            length_scale: 1.0,
            noise_w: 0.8,
        }
    }
}
impl VoiceConfig {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 1024 * 1024 {
            return Err(PiperError::TooLarge);
        }
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| PiperError::Model("invalid JSON voice configuration".into()))?;
        if !(8000..=96000).contains(&value.audio.sample_rate)
            || !(3..=65536).contains(&value.num_symbols)
            || !(1..=65536).contains(&value.num_speakers)
            || value.phoneme_id_map.is_empty()
            || value.phoneme_id_map.len() > 65536
            || value.phoneme_map.len() > 65536
            || value.phoneme_id_map.values().any(|ids| {
                ids.is_empty()
                    || ids.len() > 16
                    || ids.iter().any(|i| *i < 0 || *i >= value.num_symbols)
            })
            || value.phoneme_map.values().any(|p| p.len() > 16)
            || ['_', '^', '$']
                .iter()
                .any(|p| !value.phoneme_id_map.contains_key(p))
        {
            return Err(PiperError::Model(
                "invalid voice dimensions or phoneme mapping".into(),
            ));
        }
        if !matches!(value.phoneme_type.as_str(), "text" | "espeak") {
            return Err(PiperError::Unsupported("phoneme_type"));
        }
        if value.phoneme_type == "espeak"
            && value
                .espeak
                .as_ref()
                .is_none_or(|e| e.voice.is_empty() || e.voice.len() > 128 || e.voice.contains('\0'))
        {
            return Err(PiperError::Model("missing eSpeak voice".into()));
        }
        if value.phoneme_type == "espeak"
            && value
                .espeak
                .as_ref()
                .is_some_and(|e| e.voice == "ar" || e.voice.starts_with("ar-"))
        {
            return Err(PiperError::Unsupported("Arabic tashkeel preprocessing"));
        }
        PiperSettings {
            speaker_id: None,
            length_scale: Some(value.inference.length_scale),
            noise_scale: Some(value.inference.noise_scale),
            noise_w: Some(value.inference.noise_w),
        }
        .validate(&value)?;
        Ok(value)
    }
    pub fn phoneme_ids(&self, phonemes: &[char], max: usize) -> Result<Vec<i64>> {
        let mut ids = Vec::new();
        let mut append = |part: &[i64]| -> Result<()> {
            if ids.len().saturating_add(part.len()) > max {
                return Err(PiperError::TooLarge);
            }
            ids.extend_from_slice(part);
            Ok(())
        };
        let pad = self
            .phoneme_id_map
            .get(&'_')
            .ok_or(PiperError::Invalid("missing pad ID"))?;
        append(
            self.phoneme_id_map
                .get(&'^')
                .ok_or(PiperError::Invalid("missing BOS ID"))?,
        )?;
        append(pad)?;
        for p in phonemes {
            let fallback = [*p];
            let mapped = self
                .phoneme_map
                .get(p)
                .map_or(fallback.as_slice(), Vec::as_slice);
            for p in mapped {
                append(
                    self.phoneme_id_map
                        .get(p)
                        .ok_or(PiperError::UnknownPhoneme(*p))?,
                )?;
                append(pad)?;
            }
        }
        append(
            self.phoneme_id_map
                .get(&'$')
                .ok_or(PiperError::Invalid("missing EOS ID"))?,
        )?;
        Ok(ids)
    }
}
#[cfg(any(feature = "onnx", test))]
pub(crate) fn normalized(text: &str, max: usize) -> Result<Vec<char>> {
    let mut out = Vec::new();
    for c in text.nfd() {
        if out.len() >= max {
            return Err(PiperError::TooLarge);
        }
        out.push(c);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn voice() -> VoiceConfig {
        VoiceConfig::parse(br#"{"audio":{"sample_rate":22050},"phoneme_type":"text","num_symbols":5,"num_speakers":1,"inference":{"noise_scale":0.667,"length_scale":1,"noise_w":0.8},"phoneme_id_map":{"_": [0],"^": [1],"$": [2],"a": [3],"b": [4]}}"#).unwrap()
    }
    #[test]
    fn ids_match_piper_bos_pad_each_phoneme_eos() {
        assert_eq!(
            voice().phoneme_ids(&['a', 'b'], 7).unwrap(),
            [1, 0, 3, 0, 4, 0, 2]
        );
    }
    #[test]
    fn unknown_phonemes_and_expansion_are_explicit_errors() {
        assert!(matches!(
            voice().phoneme_ids(&['x'], 20),
            Err(PiperError::UnknownPhoneme('x'))
        ));
        assert!(matches!(
            voice().phoneme_ids(&['a', 'b'], 6),
            Err(PiperError::TooLarge)
        ));
        let mut v = voice();
        v.phoneme_map.insert('x', vec!['a', 'b']);
        assert_eq!(v.phoneme_ids(&['x'], 7).unwrap(), [1, 0, 3, 0, 4, 0, 2]);
        v.phoneme_id_map.remove(&'_');
        assert!(v.phoneme_ids(&[], 8).is_err());
    }
    #[test]
    fn normalization_has_post_expansion_bound() {
        assert_eq!(normalized("é", 2).unwrap(), ['e', '\u{301}']);
        assert!(matches!(normalized("é", 1), Err(PiperError::TooLarge)));
    }
    #[test]
    fn invalid_config_dimensions_and_sentinel_ids_are_rejected() {
        let valid = serde_json::json!({"audio":{"sample_rate":22050},"phoneme_type":"text","num_symbols":5,"num_speakers":1,"inference":{"noise_scale":0.667,"length_scale":1,"noise_w":0.8},"phoneme_id_map":{"_": [0],"^": [1],"$": [2],"a": [3]}});
        for (key, bad) in [
            ("num_symbols", serde_json::json!(0)),
            ("num_speakers", serde_json::json!(0)),
            ("phoneme_type", serde_json::json!("unknown")),
        ] {
            let mut value = valid.clone();
            value[key] = bad;
            assert!(VoiceConfig::parse(&serde_json::to_vec(&value).unwrap()).is_err());
        }
        let mut value = valid;
        value["phoneme_id_map"]["_"] = serde_json::json!([99]);
        assert!(VoiceConfig::parse(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    #[test]
    fn invalid_settings_reject_nan_and_bad_speakers() {
        for settings in [
            PiperSettings {
                speaker_id: Some(1),
                ..Default::default()
            },
            PiperSettings {
                noise_scale: Some(f32::NAN),
                ..Default::default()
            },
            PiperSettings {
                length_scale: Some(0.),
                ..Default::default()
            },
        ] {
            assert!(settings.validate(&voice()).is_err());
        }
    }
    #[test]
    fn artifact_hash_and_size_are_checked() {
        let path =
            std::env::temp_dir().join(format!("pipecat-piper-artifact-{}", std::process::id()));
        std::fs::write(&path, b"abc").unwrap();
        let mut artifact = Artifact::new(&path, sha256_bytes(b"abc"));
        assert_eq!(artifact.read(3).unwrap(), b"abc");
        assert!(matches!(artifact.read(2), Err(PiperError::TooLarge)));
        artifact.sha256 = "0".repeat(64);
        assert!(matches!(artifact.read(3), Err(PiperError::HashMismatch)));
        artifact.sha256.clear();
        assert!(matches!(artifact.read(3), Err(PiperError::Invalid(_))));
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn download_cuda_blank_voice_and_invalid_resource_bounds_fail() {
        let a = Artifact::new("unused", "0".repeat(64));
        let mut c = PiperConfig::new("voice", a.clone(), a.clone(), a);
        assert!(c.validate().is_ok());
        c.download = true;
        assert!(matches!(c.validate(), Err(PiperError::Unsupported(_))));
        c.download = false;
        c.use_cuda = true;
        assert!(matches!(c.validate(), Err(PiperError::Unsupported(_))));
        c.use_cuda = false;
        c.voice = " ".into();
        assert!(c.validate().is_err());
        c.voice = "voice".into();
        c.queue_capacity = 0;
        assert!(c.validate().is_err());
    }
    #[test]
    fn directory_fingerprint_bounds_depth_and_is_stable() {
        let root = std::env::temp_dir().join(format!("pipecat-piper-tree-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("b"), b"two").unwrap();
        std::fs::write(root.join("a"), b"one").unwrap();
        let first = sha256_directory(&root).unwrap();
        assert_eq!(first, sha256_directory(&root).unwrap());
        let mut dir = root.join("nested");
        for _ in 0..34 {
            std::fs::create_dir_all(&dir).unwrap();
            dir = dir.join("d");
        }
        assert!(matches!(sha256_directory(&root), Err(PiperError::TooLarge)));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn directory_fingerprint_rejects_root_and_nested_symlinks() {
        let root = std::env::temp_dir().join(format!("pipecat-piper-links-{}", std::process::id()));
        std::fs::create_dir_all(root.join("real")).unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join("link")).unwrap();
        assert!(matches!(
            sha256_directory(root.join("link")),
            Err(PiperError::Invalid(_))
        ));
        assert!(matches!(
            sha256_directory(&root),
            Err(PiperError::Invalid(_))
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn directory_fingerprint_rejects_ambiguous_backslash_filename() {
        let root =
            std::env::temp_dir().join(format!("pipecat-piper-ambiguous-{}", std::process::id()));
        std::fs::create_dir_all(root.join("a")).unwrap();
        std::fs::write(root.join("a/b"), b"one").unwrap();
        std::fs::write(root.join("a\\b"), b"two").unwrap();
        assert!(matches!(
            sha256_directory(&root),
            Err(PiperError::Invalid(_))
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn directory_fingerprint_bounds_empty_directory_entries() {
        let root =
            std::env::temp_dir().join(format!("pipecat-piper-entries-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        for i in 0..10001 {
            std::fs::create_dir(root.join(i.to_string())).unwrap();
        }
        assert!(matches!(sha256_directory(&root), Err(PiperError::TooLarge)));
        std::fs::remove_dir_all(root).unwrap();
    }
}
