//! Bounded PCM16 WAV playback mixed into mono transport audio.
//!
//! WAV decoding is native and deliberately rejects compressed or non-PCM16 files.
//! Each file has an inspectable load status; a failed selection passes audio through.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, read_pcm16_wav, PcmFormat};
use pipecat_core::{FrameData, ProcessorError};

use crate::AudioMixer;

#[derive(Debug, Clone)]
pub struct SoundfileMixerConfig {
    pub sound_files: BTreeMap<String, PathBuf>,
    pub default_sound: String,
    pub volume: f64,
    pub mixing: bool,
    pub loop_audio: bool,
    /// Maximum encoded bytes per file, enforced before and during reading.
    pub max_sound_bytes: usize,
    pub max_sounds: usize,
}

impl Default for SoundfileMixerConfig {
    fn default() -> Self {
        Self {
            sound_files: BTreeMap::new(),
            default_sound: String::new(),
            volume: 0.4,
            mixing: true,
            loop_audio: true,
            max_sound_bytes: 32 * 1024 * 1024,
            max_sounds: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SoundLoadStatus {
    Loaded { samples: usize },
    Failed(String),
}

struct MixerState {
    format: Option<PcmFormat>,
    sounds: BTreeMap<String, Vec<i16>>,
    statuses: BTreeMap<String, SoundLoadStatus>,
    current_sound: String,
    position: usize,
    volume: f64,
    mixing: bool,
    loop_audio: bool,
    generation: u64,
}

pub struct SoundfileMixer {
    config: SoundfileMixerConfig,
    state: Mutex<MixerState>,
}

impl SoundfileMixer {
    pub fn new(config: SoundfileMixerConfig) -> Result<Self, ProcessorError> {
        if !config.volume.is_finite() {
            return Err(ProcessorError::new("mixer volume must be finite"));
        }
        if config.max_sound_bytes < 44 || config.max_sound_bytes == usize::MAX {
            return Err(ProcessorError::new(
                "mixer WAV byte limit must be at least 44 and below usize::MAX",
            ));
        }
        if config.max_sounds == 0 || config.sound_files.len() > config.max_sounds {
            return Err(ProcessorError::new(
                "mixer sound count exceeds its configured limit",
            ));
        }
        Ok(Self {
            state: Mutex::new(MixerState {
                format: None,
                sounds: BTreeMap::new(),
                statuses: BTreeMap::new(),
                current_sound: config.default_sound.clone(),
                position: 0,
                volume: config.volume,
                mixing: config.mixing,
                loop_audio: config.loop_audio,
                generation: 0,
            }),
            config,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, MixerState>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("mixer state lock poisoned"))
    }

    pub fn load_statuses(&self) -> Result<BTreeMap<String, SoundLoadStatus>, ProcessorError> {
        Ok(self.lock()?.statuses.clone())
    }

    pub fn current_sound(&self) -> Result<String, ProcessorError> {
        Ok(self.lock()?.current_sound.clone())
    }

    pub fn position(&self) -> Result<usize, ProcessorError> {
        Ok(self.lock()?.position)
    }
}

fn load_sound(path: &PathBuf, format: PcmFormat, limit: usize) -> Result<Vec<i16>, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if file.metadata().map_err(|error| error.to_string())?.len() > limit as u64 {
        return Err(format!("WAV exceeds the {limit}-byte limit"));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > limit {
        return Err(format!("WAV exceeds the {limit}-byte limit"));
    }
    let wav = read_pcm16_wav(&bytes).map_err(|error| error.to_string())?;
    if wav.format.channels() != 1 {
        return Err("mixer sound must be mono".into());
    }
    if wav.format.sample_rate() != format.sample_rate() {
        return Err(format!(
            "sound sample rate {} does not match output rate {}",
            wav.format.sample_rate(),
            format.sample_rate()
        ));
    }
    if wav.pcm.is_empty() {
        return Err("mixer sound must contain at least one sample".into());
    }
    decode_pcm16_le(wav.pcm).map_err(|error| error.to_string())
}

#[async_trait]
impl AudioMixer for SoundfileMixer {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        if format.channels() != 1 {
            return Err(ProcessorError::new("SoundfileMixer requires mono output"));
        }
        let generation = {
            let mut state = self.lock()?;
            state.generation = state.generation.wrapping_add(1);
            state.format = None;
            state.sounds.clear();
            state.statuses.clear();
            state.position = 0;
            state.generation
        };
        let files = self.config.sound_files.clone();
        let limit = self.config.max_sound_bytes;
        let (sounds, statuses) = tokio::task::spawn_blocking(move || {
            let mut sounds = BTreeMap::new();
            let mut statuses = BTreeMap::new();
            for (name, path) in files {
                match load_sound(&path, format, limit) {
                    Ok(samples) => {
                        statuses.insert(
                            name.clone(),
                            SoundLoadStatus::Loaded {
                                samples: samples.len(),
                            },
                        );
                        sounds.insert(name, samples);
                    }
                    Err(error) => {
                        statuses.insert(name, SoundLoadStatus::Failed(error));
                    }
                }
            }
            (sounds, statuses)
        })
        .await
        .map_err(|error| ProcessorError::new(format!("WAV loading task failed: {error}")))?;
        let mut state = self.lock()?;
        if state.generation != generation {
            return Err(ProcessorError::new(
                "mixer start superseded by a newer lifecycle operation",
            ));
        }
        state.sounds = sounds;
        state.statuses = statuses;
        state.format = Some(format);
        Ok(())
    }

    async fn mix(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        let mut state = self.lock()?;
        if let Some(format) = state.format {
            format
                .validate(&audio)
                .map_err(|error| ProcessorError::new(error.to_string()))?;
        }
        if !state.mixing || state.format.is_none() || audio.is_empty() {
            return Ok(audio);
        }
        let Some(sound) = state.sounds.get(&state.current_sound) else {
            return Ok(audio);
        };
        let input =
            decode_pcm16_le(&audio).map_err(|error| ProcessorError::new(error.to_string()))?;
        let count = input.len();
        let mut start = state.position;
        if count > sound.len().saturating_sub(start) {
            if !state.loop_audio {
                return Ok(audio);
            }
            // Whole chunks restart at zero, matching the file mixer's playback contract.
            start = 0;
        }
        let output: Vec<i16> = input
            .iter()
            .enumerate()
            .map(|(index, sample)| {
                (f64::from(*sample)
                    + f64::from(sound[(start + index) % sound.len()]) * state.volume)
                    .clamp(-32768.0, 32767.0) as i16
            })
            .collect();
        let end = start + count;
        state.position = if end > sound.len() {
            end % sound.len()
        } else {
            end
        };
        Ok(encode_pcm16_le(&output))
    }

    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        match frame {
            FrameData::MixerEnable(enable) => state.mixing = *enable,
            FrameData::MixerUpdateSettings(settings) => {
                // Validate the entire update before changing any setting.
                let mut selected = None;
                let mut volume = None;
                let mut loop_audio = None;
                for (key, value) in settings {
                    match key.as_str() {
                        "sound" => {
                            let name = value.as_str().ok_or_else(|| {
                                ProcessorError::new("mixer sound must be a string")
                            })?;
                            if !self.config.sound_files.contains_key(name) {
                                return Err(ProcessorError::new(format!(
                                    "unknown mixer sound: {name}"
                                )));
                            }
                            selected = Some(name.to_owned());
                        }
                        "volume" => {
                            let gain = value
                                .as_f64()
                                .filter(|value| value.is_finite())
                                .ok_or_else(|| {
                                    ProcessorError::new("mixer volume must be a finite number")
                                })?;
                            volume = Some(gain);
                        }
                        "loop" => {
                            loop_audio =
                                Some(value.as_bool().ok_or_else(|| {
                                    ProcessorError::new("mixer loop must be boolean")
                                })?)
                        }
                        _ => {}
                    }
                }
                if let Some(selected) = selected {
                    state.current_sound = selected;
                    state.position = 0;
                }
                if let Some(volume) = volume {
                    state.volume = volume;
                }
                if let Some(loop_audio) = loop_audio {
                    state.loop_audio = loop_audio;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.generation = state.generation.wrapping_add(1);
        state.sounds.clear();
        state.statuses.clear();
        state.position = 0;
        state.format = None;
        Ok(())
    }
}
