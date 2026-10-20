// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Strict native subset of src/pipecat/evals/suite.py: manifest-relative inputs,
//! attempt-major expansion, labels, bounded concurrency and one child per run.
use crate::{
    bounded_yaml::{self, ConstructionBudget},
    matcher::ScriptResult,
    process::{run_hosted_prepared, HostedResult, NativeCommand, ProcessOptions},
    scenario::ResponseParseMode,
    suite::serialized_size,
    Cancellation, EvalError, EvalSession, RecordedAudio, RecordedScenarioFile, ScenarioFile,
    ScriptScenario, SessionOptions,
};
#[cfg(feature = "bot-audio-transcription")]
use crate::{
    native_loader::{LoaderRegistrar, LoaderRegistry},
    BotTranscriptionSettings,
};
#[cfg(feature = "user-speech")]
use crate::{
    user_speech::{PreparedHostedSpeech, UserSpeechBudget},
    user_speech_scenario::{bounded_serialized_size, load_speech_plan_bounded, SpeechPlanLimits},
    CartesiaUserSpeechSettings, CartesiaUserSpeechVoice,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
#[cfg(any(feature = "bot-audio-transcription", feature = "user-speech"))]
use std::sync::Arc;
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinSet;
const MAX_RUNS: usize = 128;
const MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    native_version: u32,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "one")]
    repeat: usize,
    #[serde(default = "default_port")]
    base_port: u16,
    #[serde(default)]
    bots_dir: PathBuf,
    #[serde(default = "scenarios_dir")]
    scenarios_dir: PathBuf,
    #[serde(default)]
    record: bool,
    cache_dir: Option<PathBuf>,
    suite: Vec<EntryInput>,
}
fn default_concurrency() -> usize {
    4
}
fn one() -> usize {
    1
}
fn default_port() -> u16 {
    7900
}
fn scenarios_dir() -> PathBuf {
    "scenarios".into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryInput {
    bot: PathBuf,
    name: Option<String>,
    scenarios: Vec<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cwd: Option<PathBuf>,
    concurrency: Option<usize>,
    runner_body: Option<BodyInput>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BodyInput {
    path: Option<PathBuf>,
    data: Option<Map<String, Value>>,
}
#[derive(Clone)]
enum Body {
    Path(PathBuf),
    Inline(Map<String, Value>),
}
#[derive(Clone)]
pub struct ReleaseJob {
    pub name: String,
    pub bot: PathBuf,
    pub scenario: ScriptScenario,
    pub attempt: usize,
    pub port: u16,
    pub concurrency: usize,
    command: NativeCommand,
    body: Option<Body>,
    recordings: Option<Vec<(usize, RecordedAudio)>>,
    #[cfg(feature = "user-speech")]
    user_speech: Option<Arc<HostedSpeechPlan>>,
    // Only settings/credentials are shared across repeats. Every executing
    // attempt owns its own model, analyzer state and HTTP configuration.
    #[cfg(feature = "bot-audio-transcription")]
    audio_response: Option<Arc<BotTranscriptionSettings>>,
}
/// Immutable source provenance; this contains no execution owner or quota.
#[cfg(feature = "user-speech")]
struct HostedSpeechPlan {
    settings: CartesiaUserSpeechSettings,
    voice: Option<CartesiaUserSpeechVoice>,
    scenario: Arc<[u8]>,
}

enum UserInputMode {
    Text,
    Recorded,
    #[cfg(feature = "user-speech")]
    Speech(Arc<CartesiaUserSpeechSettings>),
}
struct LoadedScenario {
    scenario: ScriptScenario,
    recordings: Option<Vec<(usize, RecordedAudio)>>,
    #[cfg(feature = "user-speech")]
    speech: Option<Arc<HostedSpeechPlan>>,
}

/// Loaded and validated plans contain no executable shell templates. Use load()
/// to validate every scenario and all configuration before executing any child.
pub struct ReleaseManifest {
    pub concurrency: usize,
    pub jobs: Vec<ReleaseJob>,
}
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseRun {
    pub name: String,
    pub bot: PathBuf,
    pub scenario: String,
    pub attempt: usize,
    pub port: u16,
    pub hosted: HostedResult,
}
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseResult {
    pub passed: bool,
    pub runs: Vec<ReleaseRun>,
}

fn bounded_file(path: &Path) -> Result<Vec<u8>, EvalError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(EvalError::Limit("manifest/config file bytes"));
    }
    Ok(bytes)
}
fn load_value(path: &Path, recorded: bool) -> Result<Value, EvalError> {
    let bytes = if recorded {
        crate::recorded_scenario::read_regular(path, path.parent().unwrap(), 1024 * 1024)?
    } else {
        bounded_file(path)?
    };
    let value = bounded_yaml::parse_yaml(&bytes, &mut ConstructionBudget::default(), 0)?;
    Ok(serde_json::to_value(value)?)
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
impl ReleaseManifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        Self::load_mode(path.as_ref(), UserInputMode::Text, ResponseParseMode::Text)
    }
    /// Opt into preloaded Unix PCM16 recordings in every referenced scenario.
    /// The ordinary loader and native compiled-child ownership remain unchanged.
    pub fn load_recorded(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        Self::load_mode(
            path.as_ref(),
            UserInputMode::Recorded,
            ResponseParseMode::Text,
        )
    }
    /// Opt into local hosted Cartesia input with recording overrides. Loading
    /// performs no synthesis/cache I/O; execution freezes scenarios and shares
    /// one 15,360,000-byte logical PCM allowance across all jobs and repeats.
    #[cfg(feature = "user-speech")]
    pub fn load_user_speech(
        path: impl AsRef<Path>,
        settings: CartesiaUserSpeechSettings,
    ) -> Result<Self, EvalError> {
        Self::load_mode(
            path.as_ref(),
            UserInputMode::Speech(Arc::new(settings)),
            ResponseParseMode::Text,
        )
    }
    /// Preserve explicit `response` events as spoken transcripts for every
    /// attempt, optionally with preloaded recorded user input. The complete
    /// plan is bounded before execution, with at most sixteen concurrent runs.
    /// Serialized audio scenarios total at most 4 MiB across logical repeats.
    /// Settings are retained without loading a model. `run_release` first
    /// preflights and drops one real model per distinct settings object, then
    /// loads an independent model for each attempt before launching its child.
    #[cfg(feature = "bot-audio-transcription")]
    pub fn load_audio_response(
        path: impl AsRef<Path>,
        settings: BotTranscriptionSettings,
        recorded_audio: bool,
    ) -> Result<Self, EvalError> {
        let mut manifest = Self::load_mode(
            path.as_ref(),
            if recorded_audio {
                UserInputMode::Recorded
            } else {
                UserInputMode::Text
            },
            ResponseParseMode::AudioResponse,
        )?;
        let settings = Arc::new(settings);
        for job in &mut manifest.jobs {
            job.audio_response = Some(settings.clone());
        }
        Ok(manifest)
    }
    /// Explicitly combine synthesized/recorded user input with locally
    /// transcribed bot responses. Runtime settings and immutable source plans
    /// are retained; all jobs are revalidated before native model preflight.
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    pub fn load_audio_to_audio(
        path: impl AsRef<Path>,
        speech_settings: CartesiaUserSpeechSettings,
        transcription_settings: BotTranscriptionSettings,
    ) -> Result<Self, EvalError> {
        let mut manifest = Self::load_mode(
            path.as_ref(),
            UserInputMode::Speech(Arc::new(speech_settings)),
            ResponseParseMode::AudioResponse,
        )?;
        let settings = Arc::new(transcription_settings);
        for job in &mut manifest.jobs {
            job.audio_response = Some(settings.clone());
        }
        Ok(manifest)
    }
    fn load_mode(
        path: &Path,
        input_mode: UserInputMode,
        response_mode: ResponseParseMode,
    ) -> Result<Self, EvalError> {
        let path = path.canonicalize()?;
        let base = path.parent().unwrap();
        let recorded = matches!(input_mode, UserInputMode::Recorded);
        let regular_files = !matches!(input_mode, UserInputMode::Text);
        #[cfg(feature = "bot-audio-transcription")]
        let regular_files =
            regular_files || matches!(response_mode, ResponseParseMode::AudioResponse);
        let concurrency_limit = match response_mode {
            ResponseParseMode::Text => 32,
            #[cfg(feature = "bot-audio-transcription")]
            ResponseParseMode::AudioResponse => 16,
        };
        let value = load_value(&path, regular_files)?;
        if value.get("native_version") != Some(&Value::from(1)) {
            return Err(EvalError::Unsupported("release manifests require native_version: 1 and compiled bot paths; Python spawn templates are unsupported".into()));
        }
        let input: ManifestInput = serde_json::from_value(value)?;
        if input.native_version != 1
            || !(1..=concurrency_limit).contains(&input.concurrency)
            || !(1..=MAX_RUNS).contains(&input.repeat)
            || input.suite.is_empty()
            || input.suite.len() > MAX_RUNS
        {
            return Err(invalid(if concurrency_limit == 16 {
                "audio response manifest requires 1..16 concurrency and 1..128 repeat/entries"
            } else {
                "manifest requires 1..32 concurrency and 1..128 repeat/entries"
            }));
        }
        if input.record || input.cache_dir.is_some() {
            return Err(EvalError::Unsupported(
                "release recording and audio cache".into(),
            ));
        }
        let mut expanded = Vec::new();
        let mut wav_bytes = 0usize;
        let mut audio_budget = AudioBudget::default();
        let mut pairs = HashSet::new();
        let mut caps = BTreeMap::<String, usize>::new();
        for entry in input.suite {
            let name = entry
                .name
                .unwrap_or_else(|| entry.bot.to_string_lossy().into_owned());
            if name.is_empty()
                || name.len() > 1024
                || entry.scenarios.is_empty()
                || entry.scenarios.len() > MAX_RUNS
            {
                return Err(invalid(
                    "entry needs a bounded nonempty label and 1..128 scenario files",
                ));
            }
            let cap = entry.concurrency.unwrap_or(input.concurrency);
            if !(1..=concurrency_limit).contains(&cap) {
                return Err(invalid(if concurrency_limit == 16 {
                    "audio response entry concurrency must be 1..16"
                } else {
                    "entry concurrency must be 1..32"
                }));
            }
            caps.entry(name.clone())
                .and_modify(|c| *c = (*c).min(cap))
                .or_insert(cap);
            let body = match entry.runner_body {
                None => None,
                Some(BodyInput {
                    path: Some(path),
                    data: None,
                }) => {
                    let path = base.join(path).canonicalize()?;
                    if !load_value(&path, regular_files)?.is_object() {
                        return Err(invalid("runner_body file must hold a mapping"));
                    }
                    Some(Body::Path(path))
                }
                Some(BodyInput {
                    path: None,
                    data: Some(data),
                }) => Some(Body::Inline(data)),
                _ => return Err(invalid("runner_body requires exactly one of path or data")),
            };
            if body.is_some() && !entry.args.iter().any(|arg| arg == "{runner_body}") {
                return Err(invalid(
                    "runner_body requires an explicit {runner_body} argv element",
                ));
            }
            let cwd = match (&body, &entry.cwd) {
                (Some(Body::Path(path)), None) => path.parent().unwrap().to_path_buf(),
                (_, Some(path)) => base.join(path).canonicalize()?,
                _ => base.to_path_buf(),
            };
            let bot = base.join(&input.bots_dir).join(entry.bot);
            let command = NativeCommand {
                executable: bot.clone(),
                args: entry.args,
                cwd,
                env: entry.env,
            };
            command.validate()?;
            for arg in &command.args {
                if (arg.contains('{') || arg.contains('}'))
                    && arg != "{port}"
                    && arg != "{runner_body}"
                {
                    return Err(invalid(
                        "argv only supports whole-element {port} and {runner_body} placeholders",
                    ));
                }
                if arg == "{runner_body}" && body.is_none() {
                    return Err(invalid("runner_body placeholder has no body"));
                }
            }
            for reference in entry.scenarios {
                let path = if reference.ends_with(".yaml")
                    || reference.ends_with(".yml")
                    || reference.ends_with(".json")
                {
                    base.join(&reference)
                } else {
                    base.join(&input.scenarios_dir)
                        .join(format!("{reference}.yaml"))
                };
                #[cfg(feature = "user-speech")]
                let selected = if let UserInputMode::Speech(settings) = &input_mode {
                    let (file, voices) = load_speech_plan_bounded(
                        &path,
                        response_mode,
                        true,
                        SpeechPlanLimits {
                            wav_bytes: 16 * 1024 * 1024 - wav_bytes,
                            pcm_bytes: (crate::recorded_audio::MAX_TOTAL_BYTES
                                - audio_budget.pcm_bytes)
                                / input.repeat,
                            user_turns: (128 - audio_budget.references) / input.repeat,
                            scenarios: MAX_RUNS / input.repeat - expanded.len(),
                            scenario_bytes: Some(
                                (4 * 1024 * 1024 - audio_budget.scenario_bytes) / input.repeat,
                            ),
                        },
                    )?;
                    wav_bytes = wav_bytes
                        .checked_add(file.wav_bytes)
                        .ok_or(EvalError::Limit("release recording WAV bytes"))?;
                    let scenarios = file
                        .into_parts()
                        .into_iter()
                        .zip(voices)
                        .map(|((scenario, clips), voice)| {
                            // The loader measured canonical bytes before reading
                            // media; charge repeats before retaining the snapshot.
                            audio_budget.admit_speech(
                                &scenario,
                                &clips,
                                voice.is_some(),
                                input.repeat,
                                response_mode,
                            )?;
                            let frozen = Arc::<[u8]>::from(serde_json::to_vec(&scenario)?);
                            Ok(LoadedScenario {
                                scenario,
                                recordings: Some(clips),
                                speech: Some(Arc::new(HostedSpeechPlan {
                                    settings: settings.as_ref().clone(),
                                    voice,
                                    scenario: frozen,
                                })),
                            })
                        })
                        .collect::<Result<Vec<_>, EvalError>>()?;
                    Some(scenarios)
                } else {
                    None
                };
                #[cfg(not(feature = "user-speech"))]
                let selected: Option<Vec<LoadedScenario>> = None;
                let scenarios = if let Some(scenarios) = selected {
                    scenarios
                } else if recorded {
                    let file = RecordedScenarioFile::load_bounded_with_mode(
                        &path,
                        16 * 1024 * 1024 - wav_bytes,
                        (crate::recorded_audio::MAX_TOTAL_BYTES - audio_budget.pcm_bytes)
                            / input.repeat,
                        (128 - audio_budget.references) / input.repeat,
                        response_mode,
                    )?;
                    wav_bytes = wav_bytes
                        .checked_add(file.wav_bytes)
                        .ok_or(EvalError::Limit("release recording WAV bytes"))?;
                    if wav_bytes > 16 * 1024 * 1024 {
                        return Err(EvalError::Limit("release recording WAV bytes"));
                    }
                    file.into_parts()
                        .into_iter()
                        .map(|(scenario, clips)| LoadedScenario {
                            scenario,
                            recordings: Some(clips),
                            #[cfg(feature = "user-speech")]
                            speech: None,
                        })
                        .collect::<Vec<_>>()
                } else {
                    let file = match response_mode {
                        ResponseParseMode::Text => ScenarioFile::load(path)?,
                        #[cfg(feature = "bot-audio-transcription")]
                        ResponseParseMode::AudioResponse => {
                            ScenarioFile::load_audio_response(path)?
                        }
                    };
                    file.scenarios
                        .into_iter()
                        .map(|scenario| LoadedScenario {
                            scenario,
                            recordings: None,
                            #[cfg(feature = "user-speech")]
                            speech: None,
                        })
                        .collect()
                };
                for loaded in scenarios {
                    let LoadedScenario {
                        scenario,
                        recordings,
                        #[cfg(feature = "user-speech")]
                        speech,
                    } = loaded;
                    #[cfg(feature = "user-speech")]
                    let is_speech = speech.is_some();
                    #[cfg(not(feature = "user-speech"))]
                    let is_speech = false;
                    if !is_speech {
                        if let Some(clips) = &recordings {
                            audio_budget.admit(&scenario, clips, input.repeat)?;
                        } else {
                            #[cfg(feature = "bot-audio-transcription")]
                            if matches!(response_mode, ResponseParseMode::AudioResponse) {
                                audio_budget.admit_scenario(
                                    &scenario,
                                    input.repeat,
                                    "release audio response scenario bytes",
                                )?;
                            }
                        }
                    }
                    if !pairs.insert((name.clone(), scenario.name.clone())) {
                        return Err(invalid("duplicate entry label/scenario pair"));
                    }
                    if expanded.len() >= MAX_RUNS / input.repeat {
                        return Err(EvalError::Limit("release run count"));
                    }
                    expanded.push(ReleaseJob {
                        name: name.clone(),
                        bot: bot.clone(),
                        scenario,
                        attempt: 1,
                        port: 0,
                        concurrency: cap,
                        command: command.clone(),
                        body: body.clone(),
                        recordings,
                        #[cfg(feature = "user-speech")]
                        user_speech: speech,
                        #[cfg(feature = "bot-audio-transcription")]
                        audio_response: None,
                    });
                }
            }
        }
        let mut jobs = Vec::new();
        for attempt in 1..=input.repeat {
            for job in &expanded {
                let mut job = job.clone();
                job.attempt = attempt;
                job.port = input
                    .base_port
                    .checked_add(jobs.len() as u16)
                    .filter(|p| *p != 0)
                    .ok_or_else(|| invalid("release port range overflows or starts at zero"))?;
                job.concurrency = caps[&job.name];
                jobs.push(job);
            }
        }
        Ok(Self {
            concurrency: input.concurrency,
            jobs,
        })
    }
}
// Rechecked after loading because ReleaseManifest.jobs and scenario are public.
// Charge each logical repeat even though RecordedAudio clones share Arc storage.
// Recorded and spoken-response jobs share one scenario allowance; a recorded
// spoken-response job is counted once, not once for each audio mode.
#[derive(Default)]
struct AudioBudget {
    references: usize,
    pcm_bytes: usize,
    scenario_bytes: usize,
}
impl AudioBudget {
    fn admit(
        &mut self,
        scenario: &ScriptScenario,
        clips: &[(usize, RecordedAudio)],
        repeat: usize,
    ) -> Result<(), EvalError> {
        let indices = clips
            .iter()
            .map(|(index, _)| *index)
            .collect::<HashSet<_>>();
        if indices.len() != clips.len()
            || clips.iter().any(|(index, _)| {
                scenario.turns.get(*index).is_none_or(|turn| {
                    turn.user.as_ref().is_none_or(|s| s.trim().is_empty()) || turn.dtmf.is_some()
                })
            })
            || scenario
                .turns
                .iter()
                .enumerate()
                .any(|(index, turn)| turn.user.is_some() && !indices.contains(&index))
        {
            return Err(invalid("recorded release needs exactly one recording for every spoken turn and no DTMF conflict"));
        }
        let pcm = clips
            .iter()
            .try_fold(0usize, |sum, (_, clip)| {
                sum.checked_add(clip.pcm_bytes().len())
            })
            .ok_or(EvalError::Limit("release recording PCM bytes"))?;
        for (total, count, max, label) in [
            (
                &mut self.references,
                clips.len(),
                128,
                "release recording references",
            ),
            (
                &mut self.pcm_bytes,
                pcm,
                crate::recorded_audio::MAX_TOTAL_BYTES,
                "release recording PCM bytes",
            ),
        ] {
            *total = count
                .checked_mul(repeat)
                .and_then(|n| total.checked_add(n))
                .ok_or(EvalError::Limit(label))?;
            if *total > max {
                return Err(EvalError::Limit(label));
            }
        }
        self.admit_scenario(scenario, repeat, "release recorded scenario bytes")
    }
    #[cfg(feature = "user-speech")]
    fn admit_speech(
        &mut self,
        scenario: &ScriptScenario,
        clips: &[(usize, RecordedAudio)],
        has_voice: bool,
        repeat: usize,
        response_mode: ResponseParseMode,
    ) -> Result<(), EvalError> {
        // Structural limits and bounded serialization precede validate(), which
        // otherwise allocates a JSON copy of a caller-mutated public scenario.
        if scenario.turns.len() > 1024
            || scenario
                .turns
                .iter()
                .try_fold(0usize, |sum, turn| sum.checked_add(turn.expect.len()))
                .is_none_or(|count| count > 4096)
        {
            return Err(EvalError::Limit("user speech scenario structure"));
        }
        if scenario.turns.iter().any(|turn| turn.dtmf.is_some()) || scenario.needs_images() {
            return Err(EvalError::Unsupported(
                "hosted user speech does not support DTMF or images".into(),
            ));
        }
        let mut references = 0usize;
        for turn in &scenario.turns {
            if let Some(text) = &turn.user {
                if text.len() > 16 * 1024 || text.trim().is_empty() {
                    return Err(invalid(
                        "hosted user speech requires 1..16384 byte nonempty references",
                    ));
                }
                references += 1;
            }
        }
        let indices = clips
            .iter()
            .map(|(index, _)| *index)
            .collect::<HashSet<_>>();
        if indices.len() != clips.len()
            || clips.iter().any(|(index, _)| {
                scenario
                    .turns
                    .get(*index)
                    .is_none_or(|turn| turn.user.is_none())
            })
            || (!has_voice
                && scenario
                    .turns
                    .iter()
                    .enumerate()
                    .any(|(index, turn)| turn.user.is_some() && !indices.contains(&index)))
        {
            return Err(invalid(
                "hosted user speech recording coverage/provenance is invalid",
            ));
        }
        let bytes =
            bounded_serialized_size(scenario, (4 * 1024 * 1024 - self.scenario_bytes) / repeat)?;
        match response_mode {
            ResponseParseMode::Text => scenario.validate()?,
            #[cfg(feature = "bot-audio-transcription")]
            ResponseParseMode::AudioResponse => {
                scenario.audio_response_validation_copy()?;
            }
        }
        let pcm = clips
            .iter()
            .try_fold(0usize, |sum, (_, clip)| {
                sum.checked_add(clip.pcm_bytes().len())
            })
            .ok_or(EvalError::Limit("release recording PCM bytes"))?;
        for (total, count, max, label) in [
            (
                &mut self.references,
                references,
                128,
                "release user speech references",
            ),
            (
                &mut self.pcm_bytes,
                pcm,
                crate::recorded_audio::MAX_TOTAL_BYTES,
                "release user speech PCM bytes",
            ),
            (
                &mut self.scenario_bytes,
                bytes,
                4 * 1024 * 1024,
                "release user speech scenario bytes",
            ),
        ] {
            *total = count
                .checked_mul(repeat)
                .and_then(|n| total.checked_add(n))
                .filter(|total| *total <= max)
                .ok_or(EvalError::Limit(label))?;
        }
        Ok(())
    }
    fn admit_scenario(
        &mut self,
        scenario: &ScriptScenario,
        repeat: usize,
        label: &'static str,
    ) -> Result<(), EvalError> {
        // Count through the serializer's writer, without allocating a JSON
        // copy, including when public jobs have been changed after loading.
        self.scenario_bytes = serialized_size(scenario)?
            .checked_mul(repeat)
            .and_then(|bytes| self.scenario_bytes.checked_add(bytes))
            .ok_or(EvalError::Limit(label))?;
        if self.scenario_bytes > 4 * 1024 * 1024 {
            return Err(EvalError::Limit(label));
        }
        Ok(())
    }
}

/// An owned, private per-run body file. Deleted after process/pipe teardown,
/// including cancellation and dropped futures. It is never a result artifact.
struct BodyFile(PathBuf);
impl BodyFile {
    fn create(data: &Map<String, Value>) -> Result<Self, EvalError> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        for _ in 0..128 {
            let path = std::env::temp_dir().join(format!(
                "pipecat-native-eval-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => {
                    let owned = Self(path);
                    std::fs::write(owned.path(), serde_json::to_vec(data)?)?;
                    return Ok(owned);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(invalid(
            "unable to allocate private native runner body directory",
        ))
    }
    fn path(&self) -> PathBuf {
        self.0.join("runner-body.json")
    }
}
impl Drop for BodyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl ReleaseJob {
    fn concurrency_limit(&self) -> usize {
        #[cfg(feature = "bot-audio-transcription")]
        if self.audio_response.is_some() {
            return 16;
        }
        32
    }
    #[cfg(feature = "user-speech")]
    fn response_mode(&self) -> ResponseParseMode {
        #[cfg(feature = "bot-audio-transcription")]
        if self.audio_response.is_some() {
            return ResponseParseMode::AudioResponse;
        }
        ResponseParseMode::Text
    }
    fn validation_scenario(&self) -> Result<ScriptScenario, EvalError> {
        #[cfg(feature = "bot-audio-transcription")]
        if self.audio_response.is_some() {
            return self.scenario.audio_response_validation_copy();
        }
        Ok(self.scenario.clone())
    }
    async fn run(
        self,
        session: SessionOptions,
        process: ProcessOptions,
        cancel: Cancellation,
        #[cfg(feature = "bot-audio-transcription")] loaders: LoaderRegistrar,
        #[cfg(feature = "user-speech")] speech: Option<PreparedHostedSpeech>,
    ) -> Result<ReleaseRun, EvalError> {
        if cancel.is_cancelled() {
            return Ok(self.failed("cancelled", "release run cancelled before launch"));
        }
        #[cfg(feature = "bot-audio-transcription")]
        let transcription = if let Some(settings) = &self.audio_response {
            // Cancellation remains observable while native loading finishes.
            // Its owned thread is joined before the attempt can return.
            let config = settings
                .clone()
                .build_config_owned(&cancel, &loaders)
                .await?;
            let Some(config) = config else {
                return Ok(self.failed("cancelled", "release run cancelled before launch"));
            };
            Some(config)
        } else {
            None
        };
        let mut command = self.command.clone();
        let body_file = match &self.body {
            Some(Body::Inline(data)) => Some(BodyFile::create(data)?),
            _ => None,
        };
        let body = match &self.body {
            Some(Body::Path(path)) => Some(path.clone()),
            _ => body_file.as_ref().map(BodyFile::path),
        };
        command.args = command
            .args
            .into_iter()
            .map(|arg| match arg.as_str() {
                "{port}" => self.port.to_string(),
                "{runner_body}" => body.as_ref().unwrap().to_string_lossy().into_owned(),
                _ => arg,
            })
            .collect();
        let hosted = run_hosted_prepared(
            command,
            self.scenario.clone(),
            format!("ws://127.0.0.1:{}", self.port),
            session,
            process,
            cancel,
            self.recordings.clone(),
            #[cfg(feature = "bot-audio-transcription")]
            transcription,
            #[cfg(feature = "user-speech")]
            speech,
        )
        .await?;
        Ok(self.record(hosted))
    }
    fn record(&self, hosted: HostedResult) -> ReleaseRun {
        ReleaseRun {
            name: self.name.clone(),
            bot: self.bot.clone(),
            scenario: self.scenario.name.clone(),
            attempt: self.attempt,
            port: self.port,
            hosted,
        }
    }
    fn failed(&self, kind: &str, message: &str) -> ReleaseRun {
        let mut result = ScriptResult::new(&self.scenario.name, 0);
        result.fail(crate::matcher::AssertionFailure {
            turn_index: -1,
            expectation_index: -1,
            event_name: "release".into(),
            kind: kind.into(),
            reason: message.into(),
        });
        self.record(HostedResult {
            result,
            process: Default::default(),
        })
    }
}
/// Returns input/attempt order even when completion order differs. All jobs are
/// validated before launch. Cancels and joins active runs on retention overflow.
pub async fn run_release(
    manifest: ReleaseManifest,
    session: SessionOptions,
    process: ProcessOptions,
    cancel: Cancellation,
) -> Result<ReleaseResult, EvalError> {
    run_release_with_budget(manifest, session, process, cancel, MAX_BYTES).await
}

async fn cancel_and_finish<T: 'static>(
    tasks: &mut JoinSet<T>,
    cancel: &Cancellation,
    drain_loaders: bool,
) {
    cancel.cancel();
    if drain_loaders {
        // Audio tasks may own native loaders or selected speech/cache/socket
        // owners. Ordinary errors preserve their cooperative joined cleanup.
        while tasks.join_next().await.is_some() {}
    } else {
        tasks.shutdown().await;
    }
}

/// Configurable serialized retention budget, capped at 64 MiB. Summary space
/// is reserved before launching any job. A too-small budget returns an error.
pub async fn run_release_with_budget(
    mut manifest: ReleaseManifest,
    session: SessionOptions,
    process: ProcessOptions,
    cancel: Cancellation,
    max_bytes: usize,
) -> Result<ReleaseResult, EvalError> {
    process.validate()?;
    if !(1024..=MAX_BYTES).contains(&max_bytes) {
        return Err(invalid("release result budget must be 1KiB..64MiB"));
    }
    // Public jobs can be combined across plans. Any spoken-response job keeps
    // the whole scheduler at the audio cap, even in a mixed plain/audio plan.
    let concurrency_limit = manifest
        .jobs
        .iter()
        .map(ReleaseJob::concurrency_limit)
        .min()
        .unwrap_or(32);
    if !(1..=concurrency_limit).contains(&manifest.concurrency)
        || manifest.jobs.is_empty()
        || manifest.jobs.len() > MAX_RUNS
    {
        return Err(invalid("invalid release plan size/concurrency"));
    }
    #[cfg(feature = "bot-audio-transcription")]
    let drain_loaders = manifest.jobs.iter().any(|job| job.audio_response.is_some());
    #[cfg(not(feature = "bot-audio-transcription"))]
    let drain_loaders = false;
    #[cfg(feature = "user-speech")]
    let has_speech = manifest.jobs.iter().any(|job| job.user_speech.is_some());
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    if has_speech
        && drain_loaders
        && manifest
            .jobs
            .iter()
            .any(|job| job.user_speech.is_some() != job.audio_response.is_some())
    {
        return Err(EvalError::Unsupported(
            "hosted user speech cannot combine with audio response jobs".into(),
        ));
    }
    #[cfg(feature = "user-speech")]
    let drain_loaders = drain_loaders || has_speech;
    let mut audio_budget = AudioBudget::default();
    let mut ports = HashSet::new();
    let mut identities = HashSet::new();
    let mut caps = BTreeMap::<String, usize>::new();
    for job in &manifest.jobs {
        #[cfg(feature = "user-speech")]
        if let Some(plan) = &job.user_speech {
            audio_budget.admit_speech(
                &job.scenario,
                job.recordings.as_deref().unwrap_or_default(),
                plan.voice.is_some(),
                1,
                job.response_mode(),
            )?;
            // Allocation is bounded by the just-validated remaining allowance.
            if serde_json::to_vec(&job.scenario)?.as_slice() != plan.scenario.as_ref() {
                return Err(invalid("hosted user speech scenario changed after loading"));
            }
        }
        if job.name.trim().is_empty()
            || job.name.len() > 1024
            || !(1..=MAX_RUNS).contains(&job.attempt)
            || job.bot != job.command.executable
            || !identities.insert((job.name.clone(), job.scenario.name.clone(), job.attempt))
        {
            return Err(invalid(
                "release run identity/provenance is invalid or duplicated",
            ));
        }
        caps.entry(job.name.clone())
            .and_modify(|cap| *cap = (*cap).min(job.concurrency))
            .or_insert(job.concurrency);
        if !(1..=job.concurrency_limit()).contains(&job.concurrency)
            || job.port == 0
            || !ports.insert(job.port)
        {
            return Err(invalid(if job.concurrency_limit() == 16 {
                "audio response release jobs require nonzero unique ports and 1..16 concurrency"
            } else {
                "release jobs require nonzero unique ports and 1..32 concurrency"
            }));
        }
        job.command.validate()?;
        #[cfg(feature = "user-speech")]
        let is_speech = job.user_speech.is_some();
        #[cfg(not(feature = "user-speech"))]
        let is_speech = false;
        if !is_speech {
            if let Some(clips) = &job.recordings {
                audio_budget.admit(&job.scenario, clips, 1)?;
            } else {
                #[cfg(feature = "bot-audio-transcription")]
                if job.audio_response.is_some() {
                    audio_budget.admit_scenario(
                        &job.scenario,
                        1,
                        "release audio response scenario bytes",
                    )?;
                }
            }
        }
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if let (Some(_), Some(settings)) = (&job.user_speech, &job.audio_response) {
            settings.validate_audio_to_audio()?;
            EvalSession::validate_audio_to_audio_plan(
                &job.scenario,
                &format!("ws://127.0.0.1:{}", job.port),
                &session,
                job.recordings.as_deref().unwrap_or_default(),
            )?;
            continue;
        }
        let prepared = EvalSession::new(
            job.validation_scenario()?,
            format!("ws://127.0.0.1:{}", job.port),
            session.clone(),
        )?;
        if let Some(clips) = &job.recordings {
            prepared.with_recorded_audio_inputs(clips.clone())?;
        }
    }
    for job in &mut manifest.jobs {
        job.concurrency = caps[&job.name];
    }
    let mut summaries = manifest
        .jobs
        .iter()
        .map(|job| {
            Some(job.failed(
                "limit_exceeded",
                "release result budget exceeded; full run omitted and remaining work cancelled",
            ))
        })
        .collect::<Vec<_>>();
    let sizes = summaries
        .iter()
        .map(|r| serialized_size(r.as_ref().unwrap()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut reserved = sizes.iter().sum::<usize>();
    let mut stored = serialized_size(&ReleaseResult {
        passed: false,
        runs: vec![],
    })? + manifest.jobs.len().saturating_sub(1);
    if stored + reserved > max_bytes {
        return Err(EvalError::Limit("release summary budget"));
    }
    // Exactly one shared admission counter per fully revalidated invocation.
    // All recordings, including ordinary mixed jobs and repeated Arc clips,
    // are conservatively precharged before any task, cache or HTTP owner exists.
    #[cfg(feature = "user-speech")]
    let speech_budget = if has_speech {
        Some(UserSpeechBudget::new(audio_budget.pcm_bytes)?)
    } else {
        None
    };
    // Declare the outer registry before the JoinSet: dropping the release first
    // aborts its tasks, then closes loader admission and joins all native work.
    #[cfg(feature = "bot-audio-transcription")]
    let loaders = LoaderRegistry::new();
    #[cfg(feature = "bot-audio-transcription")]
    let loader_registrar = loaders.registrar();
    #[cfg(feature = "bot-audio-transcription")]
    {
        // A fully validated plan may contain jobs from different loaded
        // manifests. Preflight each distinct settings object once, dropping its
        // real model immediately so repeat expansion retains no model instances.
        let mut preflighted = HashSet::new();
        for job in &manifest.jobs {
            if cancel.is_cancelled() {
                break;
            }
            let Some(settings) = &job.audio_response else {
                continue;
            };
            if !preflighted.insert(Arc::as_ptr(settings).addr()) {
                continue;
            }
            if settings
                .clone()
                .preflight_owned(&cancel, &loader_registrar)
                .await?
                .is_none()
            {
                break;
            }
        }
    }
    let mut results = vec![None; manifest.jobs.len()];
    let mut pending = manifest.jobs.into_iter().enumerate().collect::<Vec<_>>();
    let mut active = BTreeMap::<String, usize>::new();
    let mut tasks = JoinSet::new();
    while !pending.is_empty() || !tasks.is_empty() {
        let (index, mut result) = if cancel.is_cancelled() && !pending.is_empty() {
            // Pending jobs need ordered results, not new task/model owners.
            let (index, job) = pending.remove(0);
            (
                index,
                job.failed("cancelled", "release run cancelled before launch"),
            )
        } else {
            while !cancel.is_cancelled() && tasks.len() < manifest.concurrency {
                let Some(index) = pending.iter().position(|(_, job)| {
                    active.get(&job.name).copied().unwrap_or(0) < job.concurrency
                }) else {
                    break;
                };
                let (index, job) = pending.remove(index);
                *active.entry(job.name.clone()).or_default() += 1;
                let cancel = cancel.clone();
                let session = session.clone();
                let process = process.clone();
                #[cfg(feature = "bot-audio-transcription")]
                let loader_registrar = loader_registrar.clone();
                #[cfg(feature = "user-speech")]
                let speech = job.user_speech.as_ref().map(|plan| PreparedHostedSpeech {
                    settings: plan.settings.clone(),
                    voice: plan.voice.clone(),
                    budget: speech_budget
                        .as_ref()
                        .expect("validated speech budget")
                        .clone(),
                });
                tasks.spawn(async move {
                    let name = job.name.clone();
                    let result = if cancel.is_cancelled() {
                        job.failed("cancelled", "release run cancelled before launch")
                    } else {
                        match job
                            .clone()
                            .run(
                                session,
                                process,
                                cancel,
                                #[cfg(feature = "bot-audio-transcription")]
                                loader_registrar,
                                #[cfg(feature = "user-speech")]
                                speech,
                            )
                            .await
                        {
                            Ok(result) => result,
                            Err(_) => job.failed(
                                "process_error",
                                "native run failed during config or process cleanup",
                            ),
                        }
                    };
                    (index, name, result)
                });
            }
            // Cancellation can arrive between entering this branch and its
            // admission loop. Let the next iteration summarize the queue.
            if tasks.is_empty() && cancel.is_cancelled() && !pending.is_empty() {
                continue;
            }
            let Some(joined) = tasks.join_next().await else {
                return Err(invalid("release scheduler has no runnable job"));
            };
            let (index, name, result) = match joined {
                Ok(value) => value,
                Err(_) => {
                    cancel_and_finish(&mut tasks, &cancel, drain_loaders).await;
                    return Err(invalid("release run task failed"));
                }
            };
            *active.get_mut(&name).unwrap() -= 1;
            (index, result)
        };
        reserved -= sizes[index];
        let mut size = match serialized_size(&result) {
            Ok(size) => size,
            Err(error) => {
                cancel_and_finish(&mut tasks, &cancel, drain_loaders).await;
                return Err(error);
            }
        };
        if size > max_bytes - stored - reserved {
            result = summaries[index].take().unwrap();
            size = sizes[index];
            cancel.cancel();
        }
        stored += size;
        results[index] = Some(result);
    }
    let runs = results
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid("incomplete release results"))?;
    let result = ReleaseResult {
        passed: runs.iter().all(|r| r.hosted.result.passed),
        runs,
    };
    if serialized_size(&result)? > max_bytes {
        return Err(EvalError::Limit("release result accounting"));
    }
    Ok(result)
}

#[cfg(all(test, unix, feature = "bot-audio-transcription"))]
mod native_loader_tests {
    use super::*;
    use std::{
        cell::RefCell,
        sync::{
            atomic::{AtomicBool, AtomicUsize},
            Barrier, Condvar, Mutex,
        },
        time::Duration,
    };

    struct ThreadExit(Arc<AtomicUsize>);
    impl Drop for ThreadExit {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    thread_local! {
        static THREAD_EXIT: RefCell<Option<ThreadExit>> = const { RefCell::new(None) };
    }

    #[derive(Default)]
    struct Gate {
        entered: AtomicUsize,
        exited: Arc<AtomicUsize>,
        released: Mutex<bool>,
        wake: Condvar,
        timed_out: AtomicBool,
    }
    impl Gate {
        fn block(&self) {
            THREAD_EXIT.with(|probe| {
                *probe.borrow_mut() = Some(ThreadExit(self.exited.clone()));
            });
            self.entered.fetch_add(1, Ordering::SeqCst);
            // Bound a broken test too: unwinding an outer owner must not leave
            // the suite hanging forever on this deliberately blocked thread.
            let (released, timeout) = self
                .wake
                .wait_timeout_while(
                    self.released.lock().unwrap(),
                    Duration::from_secs(5),
                    |released| !*released,
                )
                .unwrap();
            if !*released && timeout.timed_out() {
                self.timed_out.store(true, Ordering::SeqCst);
            }
        }
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.wake.notify_all();
        }
        async fn wait_entered(&self) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while self.entered.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("native loader did not enter its fixture");
        }
        fn assert_joined(&self) {
            assert!(!self.timed_out.load(Ordering::SeqCst));
            assert_eq!(self.entered.load(Ordering::SeqCst), 1);
            // The probe runs in a thread-local destructor, after its loader
            // closure returns. Receiving a result alone cannot establish this.
            assert_eq!(self.exited.load(Ordering::SeqCst), 1);
        }
    }

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "pipecat-release-loader-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn marker(&self) -> PathBuf {
            self.0.join("child-was-spawned")
        }
        fn manifest(&self, settings: BotTranscriptionSettings) -> ReleaseManifest {
            // Use a real native executable: NativeCommand correctly rejects
            // script fixtures. Any unintended launch leaves a durable marker.
            let executable = ["/usr/bin/touch", "/bin/touch"]
                .into_iter()
                .find_map(|path| Path::new(path).canonicalize().ok())
                .expect("native touch fixture");
            let command = NativeCommand {
                executable: executable.clone(),
                args: vec![self.marker().to_string_lossy().into_owned()],
                cwd: self.0.clone(),
                env: BTreeMap::new(),
            };
            command.validate().unwrap();
            let scenario: ScriptScenario = serde_json::from_value(serde_json::json!({
                "name": "owned loader", "turns": [{
                    "user": "hello", "expect": [{
                        "event": "response", "text_contains": "hello"
                    }]
                }]
            }))
            .unwrap();
            let settings = Arc::new(settings);
            ReleaseManifest {
                concurrency: 1,
                jobs: (1..=3)
                    .map(|attempt| ReleaseJob {
                        name: "loader fixture".into(),
                        bot: executable.clone(),
                        scenario: scenario.clone(),
                        attempt,
                        port: 7900 + attempt as u16,
                        concurrency: 1,
                        command: command.clone(),
                        body: None,
                        recordings: None,
                        #[cfg(feature = "user-speech")]
                        user_speech: None,
                        audio_response: Some(settings.clone()),
                    })
                    .collect(),
            }
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn assert_cancelled(result: &ReleaseResult, temp: &Temp) {
        assert!(!result.passed);
        assert_eq!(result.runs.len(), 3);
        for (index, run) in result.runs.iter().enumerate() {
            assert_eq!(run.attempt, index + 1);
            assert_eq!(run.hosted.process.pid, None);
            assert_eq!(run.hosted.result.failures[0].kind, "cancelled");
        }
        assert!(!temp.marker().exists());
    }

    // This composition is exercised only by the combined-feature library
    // graph; the first user-speech-only suite run cannot claim its coverage.
    #[cfg(feature = "user-speech")]
    #[tokio::test(flavor = "current_thread")]
    async fn mixed_hosted_speech_and_audio_response_reject_before_any_owner_starts() {
        let temp = Temp::new();
        let preflights = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let preflight_count = preflights.clone();
        let attempt_count = attempts.clone();
        let settings = BotTranscriptionSettings::for_native_loader_test(
            move || {
                preflight_count.fetch_add(1, Ordering::SeqCst);
                Err(invalid("mixed-mode rejection admitted model preflight"))
            },
            move || {
                attempt_count.fetch_add(1, Ordering::SeqCst);
                Err(invalid("mixed-mode rejection admitted attempt model"))
            },
        );
        let mut manifest = temp.manifest(settings);
        manifest.jobs.truncate(2);
        let provider = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        provider.set_nonblocking(true).unwrap();
        let speech = CartesiaUserSpeechSettings::new(
            pipecat_speech::SpeechConfig {
                base_url: format!("http://{}", provider.local_addr().unwrap()),
                api_key: "synthetic-mixed-mode-fixture".into(),
                timeout: Duration::from_secs(1),
                max_audio_bytes: 1_920_000,
                max_json_bytes: 64 * 1024,
                queue_capacity: 1,
            },
            crate::UserSpeechCachePolicy::Disabled,
        )
        .unwrap();
        let selected = &mut manifest.jobs[0];
        selected.audio_response = None;
        selected.scenario.turns[0].expect[0].event = "llm_response".into();
        selected.scenario.validate().unwrap();
        selected.recordings = Some(Vec::new());
        selected.user_speech = Some(Arc::new(HostedSpeechPlan {
            settings: speech,
            voice: Some(CartesiaUserSpeechVoice::new("fixture-voice", None, None).unwrap()),
            scenario: Arc::from(serde_json::to_vec(&selected.scenario).unwrap()),
        }));
        let result = run_release(
            manifest,
            SessionOptions::default(),
            ProcessOptions::default(),
            Cancellation::new(),
        )
        .await;
        match result {
            Err(EvalError::Unsupported(message)) => assert_eq!(
                message,
                "hosted user speech cannot combine with audio response jobs",
            ),
            other => panic!("expected exact mixed-profile rejection, got {other:?}"),
        }
        assert_eq!(preflights.load(Ordering::SeqCst), 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(!temp.marker().exists(), "mixed plan launched a child");
        assert_eq!(
            provider.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_release_preflight_joins_before_return_without_spawning() {
        let temp = Temp::new();
        let gate = Arc::new(Gate::default());
        let preflight_gate = gate.clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempt_count = attempts.clone();
        let settings = BotTranscriptionSettings::for_native_loader_test(
            move || {
                preflight_gate.block();
                Ok(())
            },
            move || {
                attempt_count.fetch_add(1, Ordering::SeqCst);
                Err(invalid("unexpected fixture attempt"))
            },
        );
        let cancel = Cancellation::new();
        let mut run = Box::pin(run_release(
            temp.manifest(settings),
            SessionOptions::default(),
            ProcessOptions::default(),
            cancel.clone(),
        ));
        tokio::select! {
            result = &mut run => panic!("release returned before preflight: {result:?}"),
            _ = gate.wait_entered() => {}
        }
        cancel.cancel();
        let returned_before_release =
            tokio::time::timeout(Duration::from_millis(25), &mut run).await;
        gate.release();
        let waited_for_release = returned_before_release.is_err();
        let result = match returned_before_release {
            Ok(result) => result,
            Err(_) => tokio::time::timeout(Duration::from_secs(2), run)
                .await
                .expect("cancelled release did not finish after loader release"),
        };
        gate.assert_joined();
        assert!(waited_for_release);
        let result = result.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert_cancelled(&result, &temp);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_release_attempt_joins_and_leaves_pending_jobs_unstarted() {
        let temp = Temp::new();
        let gate = Arc::new(Gate::default());
        let attempt_gate = gate.clone();
        let preflights = Arc::new(AtomicUsize::new(0));
        let preflight_count = preflights.clone();
        let settings = BotTranscriptionSettings::for_native_loader_test(
            move || {
                preflight_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            move || {
                attempt_gate.block();
                Err(invalid("cancelled fixture model"))
            },
        );
        let cancel = Cancellation::new();
        let mut run = Box::pin(run_release(
            temp.manifest(settings),
            SessionOptions::default(),
            ProcessOptions::default(),
            cancel.clone(),
        ));
        tokio::select! {
            result = &mut run => panic!("release returned before attempt: {result:?}"),
            _ = gate.wait_entered() => {}
        }
        cancel.cancel();
        let returned_before_release =
            tokio::time::timeout(Duration::from_millis(25), &mut run).await;
        gate.release();
        let waited_for_release = returned_before_release.is_err();
        let result = match returned_before_release {
            Ok(result) => result,
            Err(_) => tokio::time::timeout(Duration::from_secs(2), run)
                .await
                .expect("cancelled release did not finish after loader release"),
        };
        gate.assert_joined();
        assert!(waited_for_release);
        let result = result.unwrap();
        assert_eq!(preflights.load(Ordering::SeqCst), 1);
        assert_cancelled(&result, &temp);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_release_waits_for_its_active_attempt_native_thread() {
        let temp = Temp::new();
        let gate = Arc::new(Gate::default());
        let attempt_gate = gate.clone();
        let settings = BotTranscriptionSettings::for_native_loader_test(
            || Ok(()),
            move || {
                attempt_gate.block();
                Err(invalid("dropped fixture model"))
            },
        );
        let mut run = Box::pin(run_release(
            temp.manifest(settings),
            SessionOptions::default(),
            ProcessOptions::default(),
            Cancellation::new(),
        ));
        tokio::select! {
            result = &mut run => panic!("release returned before attempt: {result:?}"),
            _ = gate.wait_entered() => {}
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let controller_dropped = dropped.clone();
        let controller_gate = gate.clone();
        // The controller is an OS thread because dropping the outer owner is
        // deliberately synchronous and must not depend on this runtime polling.
        let controller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            let returned_early = controller_dropped.load(Ordering::SeqCst);
            controller_gate.release();
            assert!(!returned_early, "outer Drop detached its native loader");
        });
        drop(run);
        dropped.store(true, Ordering::SeqCst);
        controller.join().unwrap();
        gate.assert_joined();
        assert!(!temp.marker().exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn release_drop_and_attempt_completion_share_native_join_ownership() {
        for _ in 0..8 {
            let temp = Temp::new();
            let gate = Arc::new(Gate::default());
            let attempt_gate = gate.clone();
            let settings = BotTranscriptionSettings::for_native_loader_test(
                || Ok(()),
                move || {
                    attempt_gate.block();
                    Err(invalid("finished fixture model"))
                },
            );
            let mut run = Box::pin(run_release(
                temp.manifest(settings),
                SessionOptions::default(),
                ProcessOptions::default(),
                Cancellation::new(),
            ));
            tokio::select! {
                result = &mut run => panic!("release returned before attempt: {result:?}"),
                _ = gate.wait_entered() => {}
            }
            let start = Arc::new(Barrier::new(2));
            let controller_start = start.clone();
            let controller_gate = gate.clone();
            let controller = std::thread::spawn(move || {
                controller_start.wait();
                controller_gate.release();
            });
            start.wait();
            drop(run);
            let controller_result = controller.join();
            gate.assert_joined();
            controller_result.unwrap();
            assert!(!temp.marker().exists());
        }
    }
    #[cfg(feature = "user-speech")]
    mod joint {
        use super::*;
        use serde_json::json;

        fn speech() -> CartesiaUserSpeechSettings {
            let mut config = pipecat_speech::SpeechConfig::cartesia("synthetic-joint-plan-key");
            config.base_url = "http://127.0.0.1:1".into();
            CartesiaUserSpeechSettings::new(config, crate::UserSpeechCachePolicy::Disabled).unwrap()
        }
        fn counters() -> (BotTranscriptionSettings, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let preflights = Arc::new(AtomicUsize::new(0));
            let attempts = Arc::new(AtomicUsize::new(0));
            let preflight = preflights.clone();
            let attempt = attempts.clone();
            (
                BotTranscriptionSettings::for_native_loader_test(
                    move || {
                        preflight.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    move || {
                        attempt.fetch_add(1, Ordering::SeqCst);
                        Err(invalid("synthetic attempt model boundary"))
                    },
                ),
                preflights,
                attempts,
            )
        }
        fn freeze(job: &mut ReleaseJob) {
            job.user_speech = Some(Arc::new(HostedSpeechPlan {
                settings: speech(),
                voice: Some(CartesiaUserSpeechVoice::new("fixture-voice", None, None).unwrap()),
                scenario: Arc::from(serde_json::to_vec(&job.scenario).unwrap()),
            }));
        }
        fn manifest(temp: &Temp, settings: BotTranscriptionSettings) -> ReleaseManifest {
            let mut manifest = temp.manifest(settings);
            for job in &mut manifest.jobs {
                job.recordings = Some(Vec::new());
                freeze(job);
            }
            manifest
        }
        fn source() -> Value {
            json!({"name":"joint source", "user":{"modality":"audio", "speech":{
                "service":"cartesia", "voice":"fixture-voice"
            }}, "turns":[
                {"user":"first", "expect":[{"event":"response", "text_contains":"spoken"}]},
                {"user":"second", "send_after":{"event":"response"}}
            ]})
        }
        fn file(temp: &Temp, source: Value, repeat: usize) -> PathBuf {
            let executable = ["/usr/bin/touch", "/bin/touch"]
                .into_iter()
                .find_map(|path| Path::new(path).canonicalize().ok())
                .unwrap();
            std::fs::write(
                temp.0.join("source.json"),
                serde_json::to_vec(&source).unwrap(),
            )
            .unwrap();
            let path = temp.0.join("manifest.json");
            std::fs::write(
                &path,
                serde_json::to_vec(&json!({
                    "native_version":1,"concurrency":1,"repeat":repeat,
                    "suite":[{"bot":executable,"scenarios":["source.json"]}]
                }))
                .unwrap(),
            )
            .unwrap();
            path
        }
        fn assert_zero(preflights: &AtomicUsize, attempts: &AtomicUsize, temp: &Temp) {
            assert_eq!(preflights.load(Ordering::SeqCst), 0);
            assert_eq!(attempts.load(Ordering::SeqCst), 0);
            assert!(!temp.marker().exists());
        }

        #[test]
        fn explicit_joint_loader_freezes_original_response_and_preserves_text_alias() {
            let temp = Temp::new();
            let (settings, preflights, attempts) = counters();
            let path = file(&temp, source(), 2);
            let loaded = ReleaseManifest::load_audio_to_audio(&path, speech(), settings).unwrap();
            assert_eq!(loaded.jobs.len(), 2);
            for job in &loaded.jobs {
                assert_eq!(job.scenario.turns[0].expect[0].event, "response");
                assert_eq!(
                    job.scenario.turns[1]
                        .send_after
                        .as_ref()
                        .unwrap()
                        .event
                        .as_deref(),
                    Some("response")
                );
                assert_eq!(
                    job.user_speech.as_ref().unwrap().scenario.as_ref(),
                    serde_json::to_vec(&job.scenario).unwrap()
                );
            }
            assert!(Arc::ptr_eq(
                loaded.jobs[0].audio_response.as_ref().unwrap(),
                loaded.jobs[1].audio_response.as_ref().unwrap()
            ));
            assert!(ReleaseManifest::load_user_speech(&path, speech()).is_err());
            let mut legacy = source();
            legacy["turns"][1]["send_after"]["event"] = "llm_response".into();
            let path = file(&temp, legacy, 1);
            let ordinary = ReleaseManifest::load_user_speech(path, speech()).unwrap();
            assert_eq!(
                ordinary.jobs[0].scenario.turns[0].expect[0].event,
                "llm_response"
            );
            assert_zero(&preflights, &attempts, &temp);
        }

        #[test]
        fn original_joint_scenario_and_logical_pcm_have_exact_exclusive_budgets() {
            let temp = Temp::new();
            let (settings, _, _) = counters();
            let mut scenario = manifest(&temp, settings).jobs.remove(0).scenario;
            scenario.turns.push(
                serde_json::from_value(json!({"user":"second", "send_after":{"event":"response"}}))
                    .unwrap(),
            );
            let original = serde_json::to_vec(&scenario).unwrap();
            let validation = scenario.audio_response_validation_copy().unwrap();
            assert!(serialized_size(&validation).unwrap() > original.len());
            let mut bytes = AudioBudget {
                scenario_bytes: 4 * 1024 * 1024 - original.len(),
                ..Default::default()
            };
            bytes
                .admit_speech(&scenario, &[], true, 1, ResponseParseMode::AudioResponse)
                .unwrap();
            assert_eq!(bytes.scenario_bytes, 4 * 1024 * 1024);
            assert_eq!(serde_json::to_vec(&scenario).unwrap(), original);
            assert!(bytes
                .admit_speech(&scenario, &[], true, 1, ResponseParseMode::AudioResponse)
                .is_err());
            let mut references = AudioBudget::default();
            references
                .admit_speech(&scenario, &[], true, 64, ResponseParseMode::AudioResponse)
                .unwrap();
            assert_eq!(references.references, 128);
            assert!(references
                .admit_speech(&scenario, &[], true, 1, ResponseParseMode::AudioResponse)
                .is_err());
            let clips = vec![(0, RecordedAudio::from_pcm16(&vec![0; 1_920_000]).unwrap())];
            scenario.turns.truncate(1);
            let mut pcm = AudioBudget::default();
            pcm.admit_speech(
                &scenario,
                &clips,
                false,
                8,
                ResponseParseMode::AudioResponse,
            )
            .unwrap();
            assert_eq!(pcm.pcm_bytes, crate::recorded_audio::MAX_TOTAL_BYTES);
            assert_eq!(pcm.references, 8);
            assert_eq!(pcm.scenario_bytes, 8 * serialized_size(&scenario).unwrap());
            assert!(UserSpeechBudget::new(pcm.pcm_bytes).is_ok());
            assert!(pcm
                .admit(
                    &scenario,
                    &[(0, RecordedAudio::from_pcm16(&[0, 0]).unwrap())],
                    1
                )
                .is_err());
        }

        #[tokio::test]
        async fn joint_speech_without_recordings_is_not_double_charged_as_response_only() {
            let temp = Temp::new();
            let (settings, preflights, attempts) = counters();
            let mut manifest = manifest(&temp, settings);
            manifest.jobs.truncate(1);
            let job = &mut manifest.jobs[0];
            job.recordings = None;
            job.scenario.context = vec![Map::from_iter([(
                "content".into(),
                Value::String("x".repeat(5 * 1024 * 1024 / 2)),
            )])];
            freeze(job);
            let cancel = Cancellation::new();
            cancel.cancel();
            let result = run_release(
                manifest,
                SessionOptions::default(),
                ProcessOptions::default(),
                cancel,
            )
            .await
            .unwrap();
            assert_eq!(result.runs[0].hosted.result.failures[0].kind, "cancelled");
            assert_zero(&preflights, &attempts, &temp);
        }

        #[tokio::test]
        async fn every_late_joint_source_identity_coverage_and_summary_failure_precedes_models() {
            for mutation in 0..9 {
                let temp = Temp::new();
                let (settings, preflights, attempts) = counters();
                let mut manifest = manifest(&temp, settings);
                let last = manifest.jobs.last_mut().unwrap();
                let mut max_bytes = MAX_BYTES;
                match mutation {
                    0 => last.scenario.turns[0].user = Some("changed reference".into()),
                    1 => last.scenario.turns[0].expect[0].event = "llm_response".into(),
                    2 => {
                        last.scenario.context = vec![Map::from_iter([(
                            "content".into(),
                            Value::String("x".repeat(4 * 1024 * 1024)),
                        )])]
                    }
                    3 => {
                        last.scenario.turns[0].expect[0].event = "unsupported".into();
                        freeze(last);
                    }
                    4 => {
                        last.scenario.turns[0].expect[0].eval = Some("answers reference".into());
                        freeze(last);
                    }
                    5 => last.port = 0,
                    6 => last.attempt = 0,
                    7 => {
                        last.user_speech = Some(Arc::new(HostedSpeechPlan {
                            settings: speech(),
                            voice: None,
                            scenario: Arc::from(serde_json::to_vec(&last.scenario).unwrap()),
                        }));
                    }
                    8 => max_bytes = 1024,
                    _ => unreachable!(),
                }
                let result = run_release_with_budget(
                    manifest,
                    SessionOptions::default(),
                    ProcessOptions::default(),
                    Cancellation::new(),
                    max_bytes,
                )
                .await;
                assert!(result.is_err(), "mutation {mutation}: {result:?}");
                if mutation == 8 {
                    assert!(matches!(
                        result,
                        Err(EvalError::Limit("release summary budget"))
                    ));
                }
                assert_zero(&preflights, &attempts, &temp);
            }
        }

        #[tokio::test]
        async fn joint_finite_options_and_late_smart_turn_reject_before_any_native_entry() {
            let expansions: [fn(&mut SessionOptions); 15] = [
                |o| o.transport.queue_capacity += 1,
                |o| o.transport.max_message_bytes += 1,
                |o| o.max_wire_messages += 1,
                |o| o.max_wire_bytes += 1,
                |o| o.event_limits.max_events += 1,
                |o| o.event_limits.max_event_bytes += 1,
                |o| o.event_limits.max_text_bytes += 1,
                |o| o.handshake_timeout += Duration::from_secs(1),
                |o| o.default_timeout += Duration::from_secs(1),
                |o| o.send_after_timeout += Duration::from_secs(1),
                |o| o.bot_quiet_timeout += Duration::from_secs(1),
                |o| o.max_duration += Duration::from_secs(1),
                |o| o.transport.connect_timeout += Duration::from_secs(1),
                |o| o.transport.write_timeout += Duration::from_secs(1),
                |o| o.transport.close_timeout += Duration::from_secs(1),
            ];
            for expand in expansions {
                let temp = Temp::new();
                let (settings, preflights, attempts) = counters();
                let mut options = SessionOptions::default();
                expand(&mut options);
                let result = run_release(
                    manifest(&temp, settings),
                    options,
                    ProcessOptions::default(),
                    Cancellation::new(),
                )
                .await;
                assert!(
                    matches!(result, Err(EvalError::Scenario(message)) if message == "audio-to-audio options exceed the finite profile")
                );
                assert_zero(&preflights, &attempts, &temp);
            }
            let temp = Temp::new();
            let (settings, preflights, attempts) = counters();
            let mut manifest = manifest(&temp, settings);
            let (late, late_preflights, late_attempts) = counters();
            manifest.jobs.last_mut().unwrap().audio_response =
                Some(Arc::new(late.with_smart_turn_for_test()));
            let result = run_release(
                manifest,
                SessionOptions::default(),
                ProcessOptions::default(),
                Cancellation::new(),
            )
            .await;
            assert!(
                matches!(result, Err(EvalError::Unsupported(message)) if message.contains("Smart Turn"))
            );
            assert_zero(&preflights, &attempts, &temp);
            assert_zero(&late_preflights, &late_attempts, &temp);
        }

        #[tokio::test]
        async fn joint_mixing_keeps_standalone_rejection_and_allows_plain_with_own_cap() {
            for standalone in [false, true] {
                let temp = Temp::new();
                let (settings, preflights, attempts) = counters();
                let mut manifest = manifest(&temp, settings);
                let last = manifest.jobs.last_mut().unwrap();
                if standalone {
                    last.user_speech = None;
                    last.recordings = None;
                } else {
                    last.audio_response = None;
                    last.scenario.turns[0].expect[0].event = "llm_response".into();
                    freeze(last);
                }
                assert!(matches!(
                    run_release(
                        manifest,
                        SessionOptions::default(),
                        ProcessOptions::default(),
                        Cancellation::new()
                    )
                    .await,
                    Err(EvalError::Unsupported(_))
                ));
                assert_zero(&preflights, &attempts, &temp);
            }
            for recorded in [false, true] {
                let temp = Temp::new();
                let (settings, preflights, attempts) = counters();
                let mut manifest = manifest(&temp, settings);
                manifest.concurrency = 16;
                let last = manifest.jobs.last_mut().unwrap();
                last.user_speech = None;
                last.audio_response = None;
                last.concurrency = 32;
                last.scenario.turns[0].expect[0].event = "llm_response".into();
                last.recordings =
                    recorded.then(|| vec![(0, RecordedAudio::from_pcm16(&[0, 0]).unwrap())]);
                let cancel = Cancellation::new();
                cancel.cancel();
                let result = run_release(
                    manifest,
                    SessionOptions::default(),
                    ProcessOptions::default(),
                    cancel,
                )
                .await
                .unwrap();
                assert_cancelled(&result, &temp);
                assert_zero(&preflights, &attempts, &temp);
            }
        }

        #[tokio::test]
        async fn independent_joint_plans_preflight_distinct_settings_once_then_each_attempt() {
            let temp = Temp::new();
            let (settings, preflights, attempts) = counters();
            let mut manifest = manifest(&temp, settings);
            let (other, other_preflights, other_attempts) = counters();
            manifest.jobs.last_mut().unwrap().audio_response = Some(Arc::new(other));
            let result = run_release(
                manifest,
                SessionOptions::default(),
                ProcessOptions::default(),
                Cancellation::new(),
            )
            .await
            .unwrap();
            assert!(!result.passed);
            assert_eq!(
                result
                    .runs
                    .iter()
                    .map(|run| run.attempt)
                    .collect::<Vec<_>>(),
                [1, 2, 3]
            );
            assert_eq!(preflights.load(Ordering::SeqCst), 1);
            assert_eq!(other_preflights.load(Ordering::SeqCst), 1);
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
            assert_eq!(other_attempts.load(Ordering::SeqCst), 1);
            assert!(!temp.marker().exists());
        }

        // Only the compiled unit-test harness can select this synthetic child.
        #[tokio::test]
        async fn overflow_child() {
            use base64::{engine::general_purpose::STANDARD, Engine};
            use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
            let Some(directory) = std::env::var_os("PIPECAT_JOINT_OVERFLOW_DIRECTORY") else {
                return;
            };
            let directory = PathBuf::from(directory);
            let role = std::env::var("PIPECAT_JOINT_OVERFLOW_ROLE").unwrap();
            let port: u16 = std::env::args().next_back().unwrap().parse().unwrap();
            std::fs::write(
                directory.join(format!("pid-{role}")),
                std::process::id().to_string(),
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
                .await
                .unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = WebSocketSession::accept(
                stream,
                SessionConfig {
                    close_timeout: Duration::from_millis(200),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let mut emitted = false;
            let mut cancelled = false;
            let serving = async {
                while let Some(packet) = socket.recv().await {
                    let bytes = match packet {
                        Packet::Text(text) => text.into_bytes(),
                        Packet::Binary(bytes) => bytes.to_vec(),
                    };
                    let message: Value = serde_json::from_slice(&bytes).unwrap();
                    if message["type"] == "client-ready" {
                        socket
                            .sender()
                            .send(Packet::Text(
                                json!({"label":"rtvi-ai", "type":"bot-ready", "data":{}})
                                    .to_string(),
                            ))
                            .await
                            .unwrap();
                    } else if message["type"] == "client-message"
                        && message["data"]["t"] == "eval-cancel"
                    {
                        cancelled = true;
                    } else if message["type"] == "raw-audio" && !emitted {
                        let pcm = STANDARD
                            .decode(message["data"]["base64Audio"].as_str().unwrap())
                            .unwrap();
                        if pcm.iter().all(|byte| *byte == 0) {
                            continue;
                        }
                        assert_eq!(&pcm[..2], &[7, 0]);
                        assert!(pcm[2..].iter().all(|byte| *byte == 0));
                        emitted = true;
                        // Emit both pipes while the attempt is demonstrably
                        // active. Cancellation may kill the child immediately
                        // after socket cleanup; a normal child epilogue is not
                        // part of the process owner's cleanup guarantee.
                        use std::io::Write;
                        writeln!(std::io::stdout().lock(), "duplex stdout active").unwrap();
                        std::io::stdout().lock().flush().unwrap();
                        writeln!(std::io::stderr().lock(), "duplex stderr active").unwrap();
                        std::io::stderr().lock().flush().unwrap();
                        let sender = socket.sender();
                        sender
                            .send(Packet::Text(
                                json!({"label":"rtvi-ai", "type":"bot-llm-started", "data":{}})
                                    .to_string(),
                            ))
                            .await
                            .unwrap();
                        if role == "0" {
                            // The large result cannot exist before the other
                            // attempt's real multipart STT request is held.
                            while !directory.join("held-stt-entered").exists() {
                                tokio::time::sleep(Duration::from_millis(2)).await;
                            }
                            sender.send(Packet::Text(json!({"label":"rtvi-ai", "type":"bot-llm-text", "data":{"text":format!("overflow-ready {}", "x".repeat(128 * 1024))}}).to_string())).await.unwrap();
                        } else {
                            let pcm = &include_bytes!(
                                "../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16"
                            )[..30_720];
                            sender.send(Packet::Text(json!({"label":"rtvi-ai", "type":"eval-bot-audio", "data":{"audio":STANDARD.encode(pcm), "sampleRate":16000}}).to_string())).await.unwrap();
                        }
                        sender
                            .send(Packet::Text(
                                json!({"label":"rtvi-ai", "type":"bot-llm-stopped", "data":{}})
                                    .to_string(),
                            ))
                            .await
                            .unwrap();
                    }
                }
            };
            let served = tokio::time::timeout(Duration::from_secs(20), serving).await;
            let joined = socket.cancel_and_join().await;
            assert!(served.is_ok());
            assert!(joined.is_ok());
            assert!(emitted && cancelled);
        }

        #[tokio::test(flavor = "current_thread")]
        async fn retention_overflow_drains_active_stt_and_child_pipes_before_queued_model_admission(
        ) {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let temp = Temp::new();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/v1", listener.local_addr().unwrap());
            let stop = Cancellation::new();
            let server_stop = stop.clone();
            let entered = Arc::new(AtomicBool::new(false));
            let eof = Arc::new(AtomicBool::new(false));
            let server_entered = entered.clone();
            let server_eof = eof.clone();
            let directory = temp.0.clone();
            let mut server = tokio::spawn(async move {
                let accepted = tokio::select! {
                    _ = server_stop.cancelled() => return Ok::<_, std::io::Error>(()),
                    accepted = listener.accept() => accepted?,
                };
                let (mut stream, _) = accepted;
                let request = async {
                    let mut request = Vec::new();
                    let body_at = loop {
                        let mut bytes = [0; 4096];
                        let count = stream.read(&mut bytes).await?;
                        if count == 0 || request.len() + count > 2 * 1024 * 1024 {
                            return Err(std::io::Error::other(
                                "bounded multipart request incomplete",
                            ));
                        }
                        request.extend_from_slice(&bytes[..count]);
                        if let Some(at) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                        {
                            break at + 4;
                        }
                    };
                    let header = String::from_utf8_lossy(&request[..body_at]).to_ascii_lowercase();
                    let length: usize = header
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    while request.len() < body_at + length {
                        let mut bytes = [0; 4096];
                        let count = stream.read(&mut bytes).await?;
                        if count == 0 || request.len() + count > 2 * 1024 * 1024 {
                            return Err(std::io::Error::other("multipart body incomplete"));
                        }
                        request.extend_from_slice(&bytes[..count]);
                    }
                    assert!(header.contains("multipart/form-data; boundary="));
                    assert!(header.contains("authorization: bearer synthetic-joint-owner-stt-key"));
                    assert!(request[body_at..].windows(4).any(|bytes| bytes == b"RIFF"));
                    server_entered.store(true, Ordering::SeqCst);
                    std::fs::write(directory.join("held-stt-entered"), b"complete request")?;
                    let mut byte = [0; 1];
                    if stream.read(&mut byte).await? == 0 {
                        server_eof.store(true, Ordering::SeqCst);
                    }
                    stream.shutdown().await?;
                    Ok::<_, std::io::Error>(())
                };
                tokio::select! { result = request => result, _ = server_stop.cancelled() => Ok(()) }
            });
            let preflights = Arc::new(AtomicUsize::new(0));
            let attempts = Arc::new(AtomicUsize::new(0));
            let preflight_count = preflights.clone();
            let attempt_count = attempts.clone();
            let preflight_base = base.clone();
            let settings = BotTranscriptionSettings::for_native_loader_test(
                move || {
                    preflight_count.fetch_add(1, Ordering::SeqCst);
                    drop(crate::process::hosted_speech_attempt_tests::transcription(
                        &preflight_base,
                    ));
                    Ok(())
                },
                move || {
                    attempt_count.fetch_add(1, Ordering::SeqCst);
                    Ok(crate::process::hosted_speech_attempt_tests::transcription(
                        &base,
                    ))
                },
            );
            let mut manifest = manifest(&temp, settings);
            manifest.concurrency = 2;
            // Reserve distinct ephemeral ports only during fixture assembly;
            // no parent listener survives into the direct-child launch.
            let reservations = (0..3)
                .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                .collect::<Vec<_>>();
            for (index, job) in manifest.jobs.iter_mut().enumerate() {
                job.name = format!("overflow role {index}");
                job.concurrency = 1;
                job.port = reservations[index].local_addr().unwrap().port();
                job.bot = std::env::current_exe().unwrap();
                job.command = NativeCommand {
                    executable: job.bot.clone(),
                    args: vec![
                        "--exact".into(),
                        "release::native_loader_tests::joint::overflow_child".into(),
                        "--nocapture".into(),
                        "--test-threads=1".into(),
                        "--skip".into(),
                        "{port}".into(),
                    ],
                    cwd: temp.0.clone(),
                    env: BTreeMap::from([
                        (
                            "PIPECAT_JOINT_OVERFLOW_DIRECTORY".into(),
                            temp.0.to_string_lossy().into_owned(),
                        ),
                        ("PIPECAT_JOINT_OVERFLOW_ROLE".into(), index.to_string()),
                    ]),
                };
                job.recordings = Some(vec![(0, RecordedAudio::from_pcm16(&[7, 0]).unwrap())]);
                job.scenario.turns[0].expect[0].within_ms = Some(10_000);
                if index == 0 {
                    job.scenario.turns[0].expect[0].event = "llm_response".into();
                    job.scenario.turns[0].expect[0].text_contains = Some("overflow-ready".into());
                }
                freeze(job);
            }
            drop(reservations);
            let cancel = Cancellation::new();
            let mut run = Box::pin(run_release_with_budget(
                manifest,
                SessionOptions {
                    default_timeout: Duration::from_secs(10),
                    bot_quiet_timeout: Duration::from_secs(10),
                    max_duration: Duration::from_secs(15),
                    ..Default::default()
                },
                ProcessOptions {
                    startup_timeout: Duration::from_secs(5),
                    shutdown_grace: Duration::from_secs(1),
                    output_drain_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
                cancel.clone(),
                32 * 1024,
            ));
            let first = tokio::time::timeout(Duration::from_secs(20), &mut run).await;
            let timed_out = first.is_err();
            let result = match first {
                Ok(result) => result,
                Err(_) => {
                    cancel.cancel();
                    run.await
                }
            };
            // Permit scheduling the peer EOF observer while leaving its fixture
            // stop token untouched. A fixture-induced close cannot count here.
            let peer = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
            let eof_before_fixture_stop = eof.load(Ordering::SeqCst);
            stop.cancel();
            let peer = match peer {
                Ok(peer) => peer,
                Err(_) => server.await,
            };
            assert!(!timed_out);
            assert!(peer.unwrap().is_ok());
            assert!(entered.load(Ordering::SeqCst));
            assert!(eof_before_fixture_stop);
            let result = result.unwrap();
            assert!(!result.passed);
            assert_eq!(
                result.runs[0].hosted.result.failures[0].kind,
                "limit_exceeded"
            );
            let active = &result.runs[1].hosted;
            assert!(active
                .result
                .failures
                .iter()
                .any(|failure| failure.kind == "cancelled"));
            assert!(active.process.reaped);
            assert!(!active.process.output_incomplete);
            assert!(active.process.stdout.contains("duplex stdout active"));
            assert!(active.process.stderr.contains("duplex stderr active"));
            assert_eq!(result.runs[2].hosted.result.failures[0].kind, "cancelled");
            assert!(result.runs[2].hosted.process.pid.is_none());
            assert!(!temp.0.join("pid-2").exists());
            assert_eq!(preflights.load(Ordering::SeqCst), 1);
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
            for role in [0, 1] {
                let pid: i32 = std::fs::read_to_string(temp.0.join(format!("pid-{role}")))
                    .unwrap()
                    .parse()
                    .unwrap();
                // Release has already returned after its actual direct-child
                // waits; verify neither completed PID remains waitable here.
                assert_eq!(
                    rustix::process::waitpid(
                        Some(rustix::process::Pid::from_raw(pid).unwrap()),
                        rustix::process::WaitOptions::NOHANG,
                    )
                    .unwrap_err(),
                    rustix::io::Errno::CHILD,
                );
            }
        }
    }
}
