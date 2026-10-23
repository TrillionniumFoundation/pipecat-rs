//! Bounded, owner-provisioned raw-media storage. This is not an authority issuer
//! or a conversation/event database. One reference describes one captured segment.
use crate::StreamIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::PathBuf,
    sync::Mutex,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaStoreConfig {
    /// An existing private directory, supplied by the authenticated owner.
    pub directory: PathBuf,
    pub capture_generation: u64,
    pub max_assets: usize,
    pub max_bytes: u64,
    pub max_segment_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapturedAudioRef {
    pub stream: StreamIdentity,
    pub capture_generation: u64,
    pub sequence: u64,
    pub asset_id: String,
    pub sha256: String,
    pub byte_length: u64,
    pub sample_rate: u32,
    pub num_channels: u16,
    pub start_sample: u64,
    pub end_sample: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum MediaStoreError {
    #[error("invalid media scope, reference, or quota")]
    Invalid,
    #[error("media root must be an existing private directory without symlink components")]
    UnsafeRoot,
    #[error("media asset identity or byte digest does not match")]
    Identity,
    #[error("media quota is full; retained observations must be acknowledged before release")]
    Backpressure,
    #[error("media store I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("media store state poisoned")]
    Poisoned,
}
type Result<T> = std::result::Result<T, MediaStoreError>;

#[derive(Debug)]
struct State {
    assets: BTreeMap<String, u64>,
    bytes: u64,
    high_water: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreBinding {
    stream: StreamIdentity,
    capture_generation: u64,
}
#[derive(Debug)]
pub struct MediaStore {
    config: MediaStoreConfig,
    stream: StreamIdentity,
    state: Mutex<State>,
}

fn private_root(path: &std::path::Path) -> Result<()> {
    use std::path::Component;
    if !path.is_absolute() {
        return Err(MediaStoreError::UnsafeRoot);
    }
    let mut prefix = PathBuf::new();
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err(MediaStoreError::UnsafeRoot);
        }
        prefix.push(component);
        let metadata = fs::symlink_metadata(&prefix)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(MediaStoreError::UnsafeRoot);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(MediaStoreError::UnsafeRoot);
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(MediaStoreError::UnsafeRoot)
    }
}
fn create_private(path: &std::path::Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}
fn asset_name(generation: u64, sequence: u64) -> String {
    format!("capture-{generation}-{sequence}.pcm16")
}
fn parse_name(name: &str) -> Result<(u64, u64)> {
    let mut parts = name
        .strip_prefix("capture-")
        .and_then(|s| s.strip_suffix(".pcm16"))
        .ok_or(MediaStoreError::Invalid)?
        .split('-');
    let generation = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(MediaStoreError::Invalid)?;
    let sequence = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(MediaStoreError::Invalid)?;
    if generation == 0
        || sequence == 0
        || parts.next().is_some()
        || asset_name(generation, sequence) != name
    {
        return Err(MediaStoreError::Invalid);
    }
    Ok((generation, sequence))
}

impl MediaStore {
    /// Does not create directories or change existing permissions. The owner
    /// must keep the private directory and its ancestors immutable while open.
    /// Existing assets count toward quotas across restarts; scope drift rejects.
    pub fn open(config: MediaStoreConfig, stream: StreamIdentity) -> Result<Self> {
        if config.capture_generation == 0
            || !(1..=1024).contains(&config.max_assets)
            || config.max_bytes == 0
            || config.max_bytes > 1024 * 1024 * 1024
            || config.max_segment_bytes == 0
            || config.max_segment_bytes > 2 * 1024 * 1024
            || config.max_segment_bytes as u64 > config.max_bytes
            || stream.device_generation == 0
            || stream.organ_generation == 0
            || [
                &stream.principal_id,
                &stream.device_id,
                &stream.organ_id,
                &stream.logical_session_id,
            ]
            .iter()
            .any(|id| !crate::session::valid_id(id))
        {
            return Err(MediaStoreError::Invalid);
        }
        private_root(&config.directory)?;
        let scope_path = config.directory.join("scope.json");
        let scope = serde_json::to_vec(&StoreBinding {
            stream: stream.clone(),
            capture_generation: config.capture_generation,
        })
        .map_err(|_| MediaStoreError::Invalid)?;
        match create_private(&scope_path) {
            Ok(mut file) => {
                file.write_all(&scope)?;
                file.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&scope_path)?;
                if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 4096
                {
                    return Err(MediaStoreError::Identity);
                }
                let previous: StoreBinding = serde_json::from_slice(&fs::read(&scope_path)?)
                    .map_err(|_| MediaStoreError::Identity)?;
                if previous.stream != stream
                    || config.capture_generation <= previous.capture_generation
                {
                    return Err(MediaStoreError::Identity);
                }
                let pending = config
                    .directory
                    .join(format!("scope-next-{}.json", config.capture_generation));
                let mut file = create_private(&pending)?;
                file.write_all(&scope)?;
                file.sync_all()?;
                fs::rename(pending, &scope_path)?;
            }
            Err(error) => return Err(error.into()),
        }
        let mut state = State {
            assets: BTreeMap::new(),
            bytes: 0,
            high_water: 0,
        };
        for entry in fs::read_dir(&config.directory)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| MediaStoreError::Invalid)?;
            if name == "scope.json" {
                continue;
            }
            let (generation, sequence) = parse_name(&name)?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() == 0
                || !metadata.len().is_multiple_of(2)
                || metadata.len() > config.max_segment_bytes as u64
            {
                return Err(MediaStoreError::Identity);
            }
            if generation == config.capture_generation {
                state.high_water = state.high_water.max(sequence);
            }
            state.bytes = state
                .bytes
                .checked_add(metadata.len())
                .ok_or(MediaStoreError::Invalid)?;
            state.assets.insert(name, metadata.len());
            if state.assets.len() > config.max_assets || state.bytes > config.max_bytes {
                return Err(MediaStoreError::Backpressure);
            }
        }
        Ok(Self {
            config,
            stream,
            state: Mutex::new(state),
        })
    }

    /// Captured PCM16LE, 16 kHz mono, before recognizer-added zero padding.
    /// Runs on the recognizer's owned native worker, never the CNS event loop.
    pub fn retain(&self, pcm16: &[u8], sequence: u64) -> Result<CapturedAudioRef> {
        if sequence == 0
            || pcm16.is_empty()
            || !pcm16.len().is_multiple_of(2)
            || pcm16.len() > self.config.max_segment_bytes
        {
            return Err(MediaStoreError::Invalid);
        }
        let name = asset_name(self.config.capture_generation, sequence);
        let mut state = self.state.lock().map_err(|_| MediaStoreError::Poisoned)?;
        if sequence <= state.high_water {
            return Err(MediaStoreError::Identity);
        }
        if state.assets.len() >= self.config.max_assets
            || state.bytes + pcm16.len() as u64 > self.config.max_bytes
        {
            return Err(MediaStoreError::Backpressure);
        }
        // Capacity stays reserved until the actual file operation exits. Failure
        // never produces a source reference or overwrites an existing artifact.
        let path = self.config.directory.join(&name);
        let mut file = create_private(&path)?;
        state.assets.insert(name.clone(), pcm16.len() as u64);
        state.bytes += pcm16.len() as u64;
        state.high_water = sequence;
        file.write_all(pcm16)?;
        file.sync_all()?;
        Ok(CapturedAudioRef {
            stream: self.stream.clone(),
            capture_generation: self.config.capture_generation,
            sequence,
            asset_id: name,
            sha256: format!("{:x}", Sha256::digest(pcm16)),
            byte_length: pcm16.len() as u64,
            sample_rate: 16_000,
            num_channels: 1,
            start_sample: 0,
            end_sample: pcm16.len() as u64 / 2,
        })
    }

    /// Re-read the bounded file before an owner binds a final user observation.
    /// This proves byte identity, not speaker identity, permission or hearing.
    pub fn verify(&self, source: &CapturedAudioRef) -> Result<()> {
        if source.stream != self.stream
            || parse_name(&source.asset_id)? != (source.capture_generation, source.sequence)
            || source.sample_rate != 16_000
            || source.num_channels != 1
            || source.start_sample != 0
            || source.byte_length == 0
            || source.byte_length > self.config.max_segment_bytes as u64
            || !source.byte_length.is_multiple_of(2)
            || source.end_sample != source.byte_length / 2
        {
            return Err(MediaStoreError::Identity);
        }
        let path = self.config.directory.join(&source.asset_id);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != source.byte_length
        {
            return Err(MediaStoreError::Identity);
        }
        let mut bytes = Vec::new();
        fs::File::open(path)?
            .take(source.byte_length + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != source.byte_length
            || format!("{:x}", Sha256::digest(&bytes)) != source.sha256
        {
            return Err(MediaStoreError::Identity);
        }
        Ok(())
    }

    /// Only the owner that retained the final observation may release its asset.
    /// This never alters conversation history or claims a remote tool rollback.
    pub fn release(&self, source: &CapturedAudioRef) -> Result<()> {
        self.verify(source)?;
        let mut state = self.state.lock().map_err(|_| MediaStoreError::Poisoned)?;
        let bytes = *state
            .assets
            .get(&source.asset_id)
            .ok_or(MediaStoreError::Identity)?;
        fs::remove_file(self.config.directory.join(&source.asset_id))?;
        state.assets.remove(&source.asset_id);
        state.bytes -= bytes;
        Ok(())
    }
}

#[cfg(feature = "local-speech")]
impl pipecat_whisper::AudioSourceRetainer for MediaStore {
    fn retain_source(
        &self,
        pcm16: &[u8],
        sequence: u64,
    ) -> pipecat_whisper::Result<serde_json::Value> {
        let source = self
            .retain(pcm16, sequence)
            .map_err(|e| pipecat_whisper::WhisperError::Worker(e.to_string()))?;
        serde_json::to_value(source)
            .map_err(|e| pipecat_whisper::WhisperError::Worker(e.to_string()))
    }
}
