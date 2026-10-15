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
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::task::JoinSet;
#[cfg(feature = "bot-audio-transcription")]
use {crate::BotTranscriptionSettings, std::sync::Arc};
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
    // Only settings/credentials are shared across repeats. Every executing
    // attempt owns its own model, analyzer state and HTTP configuration.
    #[cfg(feature = "bot-audio-transcription")]
    audio_response: Option<Arc<BotTranscriptionSettings>>,
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
        Self::load_mode(path.as_ref(), false, ResponseParseMode::Text)
    }
    /// Opt into preloaded Unix PCM16 recordings in every referenced scenario.
    /// The ordinary loader and native compiled-child ownership remain unchanged.
    pub fn load_recorded(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        Self::load_mode(path.as_ref(), true, ResponseParseMode::Text)
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
            recorded_audio,
            ResponseParseMode::AudioResponse,
        )?;
        let settings = Arc::new(settings);
        for job in &mut manifest.jobs {
            job.audio_response = Some(settings.clone());
        }
        Ok(manifest)
    }
    fn load_mode(
        path: &Path,
        recorded: bool,
        response_mode: ResponseParseMode,
    ) -> Result<Self, EvalError> {
        let path = path.canonicalize()?;
        let base = path.parent().unwrap();
        let regular_files = recorded;
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
                let scenarios = if recorded {
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
                        .map(|(s, clips)| (s, Some(clips)))
                        .collect::<Vec<_>>()
                } else {
                    let file = match response_mode {
                        ResponseParseMode::Text => ScenarioFile::load(path)?,
                        #[cfg(feature = "bot-audio-transcription")]
                        ResponseParseMode::AudioResponse => {
                            ScenarioFile::load_audio_response(path)?
                        }
                    };
                    file.scenarios.into_iter().map(|s| (s, None)).collect()
                };
                for (scenario, recordings) in scenarios {
                    if let Some(clips) = &recordings {
                        audio_budget.admit(&scenario, clips, input.repeat)?;
                    }
                    #[cfg(feature = "bot-audio-transcription")]
                    if recordings.is_none()
                        && matches!(response_mode, ResponseParseMode::AudioResponse)
                    {
                        audio_budget.admit_scenario(
                            &scenario,
                            input.repeat,
                            "release audio response scenario bytes",
                        )?;
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
    ) -> Result<ReleaseRun, EvalError> {
        if cancel.is_cancelled() {
            return Ok(self.failed("cancelled", "release run cancelled before launch"));
        }
        #[cfg(feature = "bot-audio-transcription")]
        let transcription = if let Some(settings) = &self.audio_response {
            // Loading and inference stay in this task's owner. They cannot be
            // bounded by an async timeout or delegated to a detached loader.
            let config = settings.build_config();
            if cancel.is_cancelled() {
                return Ok(self.failed("cancelled", "release run cancelled before launch"));
            }
            Some(config?)
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
    let mut audio_budget = AudioBudget::default();
    let mut ports = HashSet::new();
    let mut identities = HashSet::new();
    let mut caps = BTreeMap::<String, usize>::new();
    for job in &manifest.jobs {
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
        if let Some(clips) = &job.recordings {
            audio_budget.admit(&job.scenario, clips, 1)?;
        }
        #[cfg(feature = "bot-audio-transcription")]
        if job.recordings.is_none() && job.audio_response.is_some() {
            audio_budget.admit_scenario(
                &job.scenario,
                1,
                "release audio response scenario bytes",
            )?;
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
            if !preflighted.insert(Arc::as_ptr(settings)) {
                continue;
            }
            let config = settings.build_config();
            if cancel.is_cancelled() {
                break;
            }
            drop(config?);
        }
    }
    let mut results = vec![None; manifest.jobs.len()];
    let mut pending = manifest.jobs.into_iter().enumerate().collect::<Vec<_>>();
    let mut active = BTreeMap::<String, usize>::new();
    let mut tasks = JoinSet::new();
    while !pending.is_empty() || !tasks.is_empty() {
        while tasks.len() < manifest.concurrency {
            let Some(index) = pending
                .iter()
                .position(|(_, job)| active.get(&job.name).copied().unwrap_or(0) < job.concurrency)
            else {
                break;
            };
            let (index, job) = pending.remove(index);
            *active.entry(job.name.clone()).or_default() += 1;
            let cancel = cancel.clone();
            let session = session.clone();
            let process = process.clone();
            tasks.spawn(async move {
                let name = job.name.clone();
                let result = if cancel.is_cancelled() {
                    job.failed("cancelled", "release run cancelled before launch")
                } else {
                    match job.clone().run(session, process, cancel).await {
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
        let Some(joined) = tasks.join_next().await else {
            return Err(invalid("release scheduler has no runnable job"));
        };
        let (index, name, mut result) = match joined {
            Ok(value) => value,
            Err(_) => {
                cancel.cancel();
                tasks.shutdown().await;
                return Err(invalid("release run task failed"));
            }
        };
        *active.get_mut(&name).unwrap() -= 1;
        reserved -= sizes[index];
        let mut size = serialized_size(&result)?;
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
