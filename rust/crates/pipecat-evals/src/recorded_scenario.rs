//! Explicit trusted-local recorded bundles; ordinary scenario parsing is unchanged.
use crate::{
    recorded_audio::MAX_TOTAL_BYTES, scenario::ResponseParseMode, EvalError, EvalSession,
    RecordedAudio, ScenarioFile, ScriptScenario, SessionOptions,
};
use serde_json::{Map, Value};
use std::{
    io::Read,
    path::{Path, PathBuf},
};
const MAX_WAV_BYTES: usize = 1_920_000 + 65_536;
const MAX_BUNDLE_WAV_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDINGS: usize = 128;
const MAX_EXPANDED_SCENARIO_BYTES: usize = 4 * 1024 * 1024;
/// Preloaded recorded scenarios. Unix only; not a hostile-filesystem sandbox.
#[derive(Debug)]
pub struct RecordedScenarioFile {
    name: String,
    scenarios: Vec<(ScriptScenario, Vec<(usize, RecordedAudio)>)>,
    pub(crate) wav_bytes: usize,
}
impl RecordedScenarioFile {
    /// Read bounded YAML/JSON and PCM16 WAVs before any network connection.
    /// Media paths use the supplied outer file's parent directory; includes retain the
    /// ordinary native loader's per-including-file path semantics.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        Self::load_bounded(
            path.as_ref(),
            MAX_BUNDLE_WAV_BYTES,
            MAX_TOTAL_BYTES,
            MAX_RECORDINGS,
        )
    }
    /// Preserve explicit audio response events while retaining all recorded
    /// bundle checks and validating before reading recording payloads.
    #[cfg(feature = "bot-audio-transcription")]
    pub(crate) fn load_audio_response(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        Self::load_bounded_with_mode(
            path.as_ref(),
            MAX_BUNDLE_WAV_BYTES,
            MAX_TOTAL_BYTES,
            MAX_RECORDINGS,
            ResponseParseMode::AudioResponse,
        )
    }
    pub(crate) fn load_bounded(
        path: &Path,
        wav_limit: usize,
        pcm_limit: usize,
        reference_limit: usize,
    ) -> Result<Self, EvalError> {
        Self::load_bounded_with_mode(
            path,
            wav_limit,
            pcm_limit,
            reference_limit,
            ResponseParseMode::Text,
        )
    }
    pub(crate) fn load_bounded_with_mode(
        path: &Path,
        wav_limit: usize,
        pcm_limit: usize,
        reference_limit: usize,
        mode: ResponseParseMode,
    ) -> Result<Self, EvalError> {
        Self::load_with_transform(
            path,
            wav_limit,
            pcm_limit,
            reference_limit,
            mode,
            extract_recordings,
        )
    }
    /// Shared bounded preflight and recording loading; the caller supplies only
    /// its opt-in scenario normalization, before any media payload is read.
    pub(crate) fn load_with_transform(
        path: &Path,
        wav_limit: usize,
        pcm_limit: usize,
        reference_limit: usize,
        mode: ResponseParseMode,
        mut transform: impl FnMut(&mut Map<String, Value>) -> Result<Vec<(usize, PathBuf)>, EvalError>,
    ) -> Result<Self, EvalError> {
        let wav_limit = wav_limit.min(MAX_BUNDLE_WAV_BYTES);
        let pcm_limit = pcm_limit.min(MAX_TOTAL_BYTES);
        let reference_limit = reference_limit.min(MAX_RECORDINGS);
        ensure_supported()?;
        let supplied = path;
        let root = supplied
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .canonicalize()?;
        let path = supplied.canonicalize()?;
        let root = root.as_path();
        let value = crate::scenario::load_recorded_value(&path, root)?;
        let mut pending = Vec::new();
        let mut count = 0usize;
        let mut expanded_bytes = 0usize;
        let file = ScenarioFile::from_value_with_mode(value, mode, |raw| {
            expanded_bytes = expanded_bytes
                .checked_add(serde_json::to_vec(raw)?.len())
                .ok_or(EvalError::Limit("expanded recorded scenario bytes"))?;
            if expanded_bytes > MAX_EXPANDED_SCENARIO_BYTES {
                return Err(EvalError::Limit("expanded recorded scenario bytes"));
            }
            let paths = transform(raw)?;
            count = count
                .checked_add(paths.len())
                .ok_or(EvalError::Limit("recording references"))?;
            if count > reference_limit {
                return Err(EvalError::Limit("recording references"));
            }
            pending.push(paths);
            Ok(())
        })?;
        // Validate all expanded references before reading any recording payload.
        let mut planned_bytes = 0usize;
        let mut canonical = Vec::with_capacity(pending.len());
        for paths in pending {
            let mut resolved = Vec::with_capacity(paths.len());
            for (index, path) in paths {
                let p = contained_path(&root.join(path), root)?;
                let meta = std::fs::metadata(&p)?;
                if !meta.is_file() {
                    return Err(invalid("recordings must be regular files"));
                }
                let size = usize::try_from(meta.len())
                    .map_err(|_| EvalError::Limit("recording WAV bytes"))?;
                if size > MAX_WAV_BYTES {
                    return Err(EvalError::Limit("recording WAV bytes"));
                }
                planned_bytes = planned_bytes
                    .checked_add(size)
                    .ok_or(EvalError::Limit("recording bundle bytes"))?;
                if planned_bytes > wav_limit {
                    return Err(EvalError::Limit("recording bundle bytes"));
                }
                resolved.push((index, p));
            }
            canonical.push(resolved);
        }
        let mut actual_bytes = 0usize;
        let mut pcm_bytes = 0usize;
        let mut scenarios = Vec::new();
        for (scenario, paths) in file.scenarios.into_iter().zip(canonical) {
            let mut clips = Vec::with_capacity(paths.len());
            for (index, path) in paths {
                let bytes = read_regular(&path, root, MAX_WAV_BYTES.min(wav_limit - actual_bytes))?;
                actual_bytes = actual_bytes
                    .checked_add(bytes.len())
                    .ok_or(EvalError::Limit("recording bundle bytes"))?;
                if actual_bytes > wav_limit {
                    return Err(EvalError::Limit("recording bundle bytes"));
                }
                // Inspect borrowed PCM before allocating owned clip bytes. A suite
                // supplies its remaining aggregate allowance to this same loader.
                let parsed = pipecat_audio::read_pcm16_wav(&bytes)
                    .map_err(|e| invalid(&format!("recording WAV: {e}")))?;
                if parsed.pcm.len() > pcm_limit - pcm_bytes {
                    return Err(EvalError::Limit("recording PCM bundle bytes"));
                }
                let clip = RecordedAudio::from_wav(&bytes)?;
                pcm_bytes = pcm_bytes
                    .checked_add(clip.pcm_bytes().len())
                    .ok_or(EvalError::Limit("recording PCM bundle bytes"))?;
                if pcm_bytes > pcm_limit {
                    return Err(EvalError::Limit("recording PCM bundle bytes"));
                }
                clips.push((index, clip));
            }
            scenarios.push((scenario, clips));
        }
        Ok(Self {
            name: file.name,
            scenarios,
            wav_bytes: actual_bytes,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn len(&self) -> usize {
        self.scenarios.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scenarios.is_empty()
    }
    pub(crate) fn into_parts(self) -> Vec<(ScriptScenario, Vec<(usize, RecordedAudio)>)> {
        self.scenarios
    }
    /// Construct all sessions without connecting; recordings are already owned.
    pub fn into_sessions(
        self,
        url: &str,
        options: SessionOptions,
    ) -> Result<Vec<EvalSession>, EvalError> {
        self.scenarios
            .into_iter()
            .map(|(scenario, clips)| {
                EvalSession::new(scenario, url, options.clone())?.with_recorded_audio_inputs(clips)
            })
            .collect()
    }
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
fn extract_recordings(raw: &mut Map<String, Value>) -> Result<Vec<(usize, PathBuf)>, EvalError> {
    let block = raw
        .get_mut("user")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("recorded scenarios require user.modality: audio"))?;
    if block.get("modality").and_then(Value::as_str) != Some("audio") {
        return Err(invalid("recorded scenarios require user.modality: audio"));
    }
    if block.keys().any(|key| key != "modality") {
        return Err(EvalError::Unsupported(
            "recorded scenarios do not synthesize speech or configure user providers".into(),
        ));
    }
    block.insert("modality".into(), "text".into());
    let turns = raw
        .get_mut("turns")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("turns must be a list"))?;
    let mut paths = Vec::new();
    for (index, turn) in turns.iter_mut().enumerate() {
        let turn = turn
            .as_object_mut()
            .ok_or_else(|| invalid("turn must be a mapping"))?;
        let audio = turn.remove("audio").filter(|v| !v.is_null());
        let user = turn.get("user").filter(|v| !v.is_null());
        if let Some(audio) = audio {
            let path = audio
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| invalid("turn.audio must be a nonempty local WAV path"))?;
            if path.len() > 4096 {
                return Err(EvalError::Limit("recording path bytes"));
            }
            if user
                .and_then(Value::as_str)
                .is_none_or(|s| s.trim().is_empty())
            {
                return Err(invalid("recording requires a nonempty user reference"));
            }
            if turn.get("dtmf").is_some_and(|v| !v.is_null()) {
                return Err(invalid("recording and dtmf are mutually exclusive"));
            }
            paths.push((index, PathBuf::from(path)));
        } else if user.is_some() {
            return Err(EvalError::Unsupported("every user turn needs a recording; speech synthesis and text fallback are disabled".into()));
        }
    }
    Ok(paths)
}
fn ensure_supported() -> Result<(), EvalError> {
    if cfg!(unix) {
        Ok(())
    } else {
        Err(EvalError::Unsupported(
            "recorded scenario files currently require Unix regular-file semantics".into(),
        ))
    }
}
fn contained_path(path: &Path, root: &Path) -> Result<PathBuf, EvalError> {
    let path = path.canonicalize()?;
    if !path.starts_with(root) {
        return Err(invalid(
            "recorded bundle paths must stay beneath the outer scenario directory",
        ));
    }
    Ok(path)
}
/// Static canonical containment is not a race-proof filesystem sandbox.
/// O_NONBLOCK prevents a swapped-in FIFO from blocking before fstat rejects it.
pub(crate) fn read_regular(path: &Path, root: &Path, limit: usize) -> Result<Vec<u8>, EvalError> {
    ensure_supported()?;
    let path = contained_path(path, root)?;
    let before = std::fs::metadata(&path)?;
    if !before.is_file() {
        return Err(invalid("recorded bundle inputs must be regular files"));
    }
    if before.len() > limit as u64 {
        return Err(EvalError::Limit("recorded bundle file bytes"));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(&path)?
    };
    #[cfg(not(unix))]
    let file = std::fs::File::open(&path)?; // unreachable: ensure_supported rejects above.
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(invalid("recorded bundle inputs must be regular files"));
    }
    if metadata.len() > limit as u64 {
        return Err(EvalError::Limit("recorded bundle file bytes"));
    }
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(EvalError::Limit("recorded bundle file bytes"));
    }
    Ok(bytes)
}
