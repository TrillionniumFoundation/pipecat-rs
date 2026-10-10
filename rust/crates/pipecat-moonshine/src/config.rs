use crate::{MoonshineError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet, fs::File, io::Read, path::PathBuf, str::FromStr, sync::OnceLock,
    time::Duration,
};

pub const SAMPLE_RATE: u32 = 16_000;
pub(crate) const LIBRARY_SHA256: &str =
    "673e4193d053a584bdfb5238c4abeaca1c8358ca47d5e495559e4791714257c2";
pub(crate) const RUNTIME_SHA256: &str =
    "18624ce60bd5d6e7c586d7fc1e96bf4c2774178ee000aea36e6af99fd6431cce";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Language(String);
impl Language {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for Language {
    type Err = MoonshineError;
    fn from_str(value: &str) -> Result<Self> {
        if value.len() > 35
            || !value
                .split('-')
                .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            return Err(MoonshineError::Invalid("invalid language tag"));
        }
        let code = value
            .split('-')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let code = if code == "fil" { "tl".to_owned() } else { code };
        if !catalog().languages.iter().any(|l| l.code == code) {
            return Err(MoonshineError::UnsupportedLanguage(value.into()));
        }
        Ok(Self(code))
    }
}
impl TryFrom<String> for Language {
    type Error = MoonshineError;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}
impl From<Language> for String {
    fn from(value: Language) -> Self {
        value.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[repr(u32)]
pub enum Model {
    Tiny = 0,
    Base = 1,
    TinyStreaming = 2,
    BaseStreaming = 3,
    #[default]
    SmallStreaming = 4,
    MediumStreaming = 5,
}
impl Model {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tiny => "tiny",
            Self::Base => "base",
            Self::TinyStreaming => "tiny-streaming",
            Self::BaseStreaming => "base-streaming",
            Self::SmallStreaming => "small-streaming",
            Self::MediumStreaming => "medium-streaming",
        }
    }
    pub fn is_streaming(self) -> bool {
        !matches!(self, Self::Tiny | Self::Base)
    }
    fn from_arch(value: u32) -> Self {
        match value {
            0 => Self::Tiny,
            1 => Self::Base,
            2 => Self::TinyStreaming,
            3 => Self::BaseStreaming,
            4 => Self::SmallStreaming,
            5 => Self::MediumStreaming,
            _ => unreachable!("pinned catalog"),
        }
    }
}
#[derive(Deserialize)]
struct Catalog {
    languages: Vec<CatalogLanguage>,
}
#[derive(Deserialize)]
struct CatalogLanguage {
    code: String,
    models: Vec<CatalogModel>,
}
#[derive(Deserialize)]
struct CatalogModel {
    model_arch: u32,
}
fn catalog() -> &'static Catalog {
    static VALUE: OnceLock<Catalog> = OnceLock::new();
    VALUE
        .get_or_init(|| serde_json::from_str(include_str!("catalog.json")).expect("pinned catalog"))
}

/// Language/model are snapshotted when a segment is admitted. Unsupported model
/// architecture for a supported language resolves to the vendor's first model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoonshineSettings {
    pub model: Model,
    pub language: Language,
}
impl Default for MoonshineSettings {
    fn default() -> Self {
        Self {
            model: Model::SmallStreaming,
            language: Language("en".into()),
        }
    }
}
impl MoonshineSettings {
    pub fn resolved(&self) -> Self {
        let language = catalog()
            .languages
            .iter()
            .find(|l| l.code == self.language.as_str())
            .expect("validated language");
        let model = language
            .models
            .iter()
            .find(|m| m.model_arch == self.model as u32)
            .unwrap_or(&language.models[0]);
        Self {
            model: Model::from_arch(model.model_arch),
            language: self.language.clone(),
        }
    }
    pub fn requires_community_license(&self) -> bool {
        !self.model.is_streaming() && self.language.as_str() != "en"
    }
}

/// Existing trusted local artifact. SHA-256 is required, never learned implicitly
/// from the file being loaded. Libraries must not be modified while in use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub(crate) fn validate(&self) -> Result<()> {
        if self.path.as_os_str().is_empty()
            || self.sha256.len() != 64
            || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(MoonshineError::Invalid(
                "local artifact path and hexadecimal SHA-256 are required",
            ));
        }
        Ok(())
    }
    pub(crate) fn read(&self, max: usize) -> Result<Vec<u8>> {
        self.validate()?;
        let file = File::open(&self.path)?;
        let len = file.metadata()?.len();
        if len == 0 || len > max as u64 {
            return Err(MoonshineError::TooLarge);
        }
        let mut bytes = Vec::with_capacity(len as usize);
        file.take(max as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.is_empty() || bytes.len() > max {
            return Err(MoonshineError::TooLarge);
        }
        if format!("{:x}", Sha256::digest(&bytes)) != self.sha256.to_ascii_lowercase() {
            return Err(MoonshineError::HashMismatch);
        }
        Ok(bytes)
    }
}

/// The audited Linux x86_64 vendor 0.1.5 wheel libraries. Their exact hashes and
/// SONAME are pinned because C structure layout is not fully version-negotiated.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLibraries {
    pub moonshine: Artifact,
    pub onnx_runtime: Artifact,
}
impl NativeLibraries {
    pub fn from_wheel_directory(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            moonshine: Artifact::new(root.join("moonshine_voice/libmoonshine.so"), LIBRARY_SHA256),
            onnx_runtime: Artifact::new(
                root.join("moonshine_voice.libs/libonnxruntime-13ab8084.so.1"),
                RUNTIME_SHA256,
            ),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if !cfg!(all(
            target_os = "linux",
            target_arch = "x86_64",
            target_pointer_width = "64"
        )) {
            return Err(MoonshineError::Invalid(
                "only the audited Linux x86_64 Moonshine 0.1.5 ABI is supported",
            ));
        }
        self.moonshine.validate()?;
        self.onnx_runtime.validate()?;
        if !self.moonshine.sha256.eq_ignore_ascii_case(LIBRARY_SHA256)
            || !self
                .onnx_runtime
                .sha256
                .eq_ignore_ascii_case(RUNTIME_SHA256)
        {
            return Err(MoonshineError::Invalid(
                "native libraries must match the audited Moonshine 0.1.5 wheel",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelFile {
    pub name: String,
    pub artifact: Artifact,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBundle {
    pub language: Language,
    pub model: Model,
    pub files: Vec<ModelFile>,
    /// Caller asserts an independently obtained grant permits using this legacy
    /// non-English model. This library never accepts legal terms on their behalf.
    #[serde(default)]
    pub community_license_granted: bool,
}
impl ModelBundle {
    pub(crate) fn validate(&self) -> Result<()> {
        let settings = MoonshineSettings {
            language: self.language.clone(),
            model: self.model,
        };
        if settings.resolved() != settings {
            return Err(MoonshineError::Invalid(
                "bundle architecture is not published for this language",
            ));
        }
        if settings.requires_community_license() && !self.community_license_granted {
            return Err(MoonshineError::LicenseRequired);
        }
        let required: &[&str] = if self.model.is_streaming() {
            &[
                "adapter.ort",
                "cross_kv.ort",
                "decoder_kv.ort",
                "encoder.ort",
                "streaming_config.json",
                "tokenizer.bin",
            ]
        } else {
            &[
                "encoder_model.ort",
                "decoder_model_merged.ort",
                "tokenizer.bin",
            ]
        };
        let names: BTreeSet<&str> = self.files.iter().map(|f| f.name.as_str()).collect();
        if names.len() != self.files.len()
            || self.files.len() > 9
            || required.iter().any(|f| !names.contains(f))
        {
            return Err(MoonshineError::Invalid("duplicate or missing model assets"));
        }
        for file in &self.files {
            if !required.contains(&file.name.as_str())
                && !(self.model.is_streaming()
                    && ["frontend.ort", "frontend.model.ort", "frontend.weights.ort"]
                        .contains(&file.name.as_str()))
            {
                return Err(MoonshineError::Invalid("unexpected model asset filename"));
            }
            file.artifact.validate()?;
        }
        if self.model.is_streaming() {
            let whole = names.contains("frontend.ort");
            let model = names.contains("frontend.model.ort");
            let weights = names.contains("frontend.weights.ort");
            if !((whole && !model && !weights) || (!whole && model && weights)) {
                return Err(MoonshineError::Invalid(
                    "streaming frontend requires either whole file or complete split pair",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MoonshineConfig {
    pub libraries: NativeLibraries,
    pub models: Vec<ModelBundle>,
    pub initial_settings: MoonshineSettings,
    /// Includes queued, running and completed-but-uncollected requests.
    pub queue_capacity: usize,
    pub max_segment_duration: Duration,
    /// Total verified bytes retained by each loaded model, including tokenizer.
    pub max_model_bytes: usize,
    pub max_transcript_bytes: usize,
}
impl MoonshineConfig {
    pub fn new(libraries: NativeLibraries, models: Vec<ModelBundle>) -> Self {
        Self {
            libraries,
            models,
            initial_settings: MoonshineSettings::default(),
            queue_capacity: 4,
            max_segment_duration: Duration::from_secs(60),
            max_model_bytes: 1024 * 1024 * 1024,
            max_transcript_bytes: 1024 * 1024,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.libraries.validate()?;
        if !(1..=1024).contains(&self.queue_capacity)
            || self.max_samples() == 0
            || self.max_segment_duration > Duration::from_secs(600)
            || !(1..=2_147_483_648).contains(&self.max_model_bytes)
            || !(1..=16_777_216).contains(&self.max_transcript_bytes)
            || self.models.is_empty()
            || self.models.len() > 60
        {
            return Err(MoonshineError::Invalid(
                "invalid queue, duration, model or transcript bounds",
            ));
        }
        let mut unique = BTreeSet::new();
        for bundle in &self.models {
            bundle.validate()?;
            if !unique.insert((bundle.language.as_str(), bundle.model as u32)) {
                return Err(MoonshineError::Invalid("duplicate model bundle"));
            }
        }
        self.bundle(&self.initial_settings)?;
        Ok(())
    }
    pub(crate) fn bundle(&self, settings: &MoonshineSettings) -> Result<&ModelBundle> {
        let settings = settings.resolved();
        self.models
            .iter()
            .find(|b| b.model == settings.model && b.language == settings.language)
            .ok_or_else(|| {
                MoonshineError::MissingModel(format!(
                    "{}/{}",
                    settings.language.as_str(),
                    settings.model.as_str()
                ))
            })
    }
    pub(crate) fn max_samples(&self) -> usize {
        (self.max_segment_duration.as_secs_f64() * f64::from(SAMPLE_RATE)).floor() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_language_aliases_and_fallback_match_vendor_catalog() {
        assert_eq!(MoonshineSettings::default().model, Model::SmallStreaming);
        for (input, expected) in [
            ("en-US", "en"),
            ("ES-mx", "es"),
            ("fil-PH", "tl"),
            ("zh-CN", "zh"),
        ] {
            assert_eq!(input.parse::<Language>().unwrap().as_str(), expected);
        }
        for input in ["", "auto", "fr", "en_uk", "en--US", "english", "en\0"] {
            assert!(input.parse::<Language>().is_err());
        }
        let mut settings = MoonshineSettings {
            model: Model::BaseStreaming,
            ..Default::default()
        };
        assert_eq!(settings.resolved().model, Model::MediumStreaming);
        settings.language = "ko".parse().unwrap();
        assert_eq!(settings.resolved().model, Model::Tiny);
        assert!(settings.resolved().requires_community_license());
        assert!(serde_json::from_str::<MoonshineSettings>(
            r#"{"model":"small-streaming","language":null}"#
        )
        .is_err());
    }
}

#[cfg(test)]
mod artifact_tests {
    use super::*;
    #[test]
    fn artifact_reader_requires_pin_and_bound() {
        let path = std::env::temp_dir().join(format!("moonshine-artifact-{}", std::process::id()));
        std::fs::write(&path, b"abc").unwrap();
        let good = Artifact::new(
            &path,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        );
        assert_eq!(good.read(3).unwrap(), b"abc");
        assert!(matches!(good.read(2), Err(MoonshineError::TooLarge)));
        assert!(matches!(
            Artifact::new(&path, "0".repeat(64)).read(3),
            Err(MoonshineError::HashMismatch)
        ));
        assert!(Artifact::new(&path, "").read(3).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn model_manifest_filenames_license_and_complete_frontend_are_enforced() {
        let mut bundle = ModelBundle {
            language: "en".parse().unwrap(),
            model: Model::Tiny,
            community_license_granted: false,
            files: [
                "encoder_model.ort",
                "decoder_model_merged.ort",
                "tokenizer.bin",
            ]
            .into_iter()
            .map(|name| ModelFile {
                name: name.into(),
                artifact: Artifact::new(name, "0".repeat(64)),
            })
            .collect(),
        };
        bundle.validate().unwrap();
        bundle.language = "ko".parse().unwrap();
        assert!(matches!(
            bundle.validate(),
            Err(MoonshineError::LicenseRequired)
        ));
        bundle.community_license_granted = true;
        bundle.validate().unwrap();
        bundle.files.push(bundle.files[0].clone());
        assert!(bundle.validate().is_err());
        bundle.files.pop();
        bundle.files[0].name = "../../weights".into();
        assert!(bundle.validate().is_err());
    }
}
