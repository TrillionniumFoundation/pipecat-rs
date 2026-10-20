//! Non-interactive init commands with explicit CLI-over-JSON precedence.
use crate::{ProjectConfig, ProjectError, ProjectPlan};
use std::{collections::BTreeMap, fs::File, io::Read, path::PathBuf};

pub const HELP: &str = "pipecat-init [NAME] --source-rust PATH [OPTIONS]\n\nGenerate a native Rust browser voice agent using an existing local Pipecat workspace.\n\n  --help, -h             Show help without writing files or reading credentials\n  --list-options         Print supported native integrations as JSON\n  --name NAME            Project name (or use positional NAME)\n  --parent DIRECTORY     Existing parent directory (default current directory)\n  --source-rust PATH     Existing workspace containing crates/pipecat-runner\n  --config FILE          Strict JSON ProjectConfig; explicit flags override it\n  --transport websocket|smallwebrtc  Native transport (default websocket)\n  --mode cascade        Deepgram STT → OpenAI chat completions → Cartesia TTS\n  --stt deepgram         Supported native runner STT\n  --llm openai           Supported native runner LLM\n  --tts cartesia         Supported native runner TTS\n  --client rust-wasm     Include Rust/WASM client source (default none)\n  --eval none|text      Add a separate native text-eval starter (default none)\n  --deploy-to-cloud      Explicitly rejected: native deployment is not implemented\n  --no-deploy-to-cloud   Explicit local-only configuration\n  --dry-run             Validate and show planned paths without creating anything\n\nThe destination must not exist, even if empty. No overwrite option is available.\nCtrl-C cooperatively cancels generation and cleans up this invocation's files.\nNo credentials, .env, build outputs, remote dependencies, or Python are copied.\n";

#[derive(Debug)]
pub enum Command {
    Help,
    ListOptions,
    Generate {
        plan: Box<ProjectPlan>,
        dry_run: bool,
    },
}
impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, ProjectError> {
        let mut args = args.into_iter();
        let mut values = BTreeMap::new();
        let mut dry_run = false;
        let mut cloud = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(Self::Help),
                "--list-options" => return Ok(Self::ListOptions),
                "--dry-run" if !dry_run => dry_run = true,
                "--deploy-to-cloud" | "--no-deploy-to-cloud" if cloud.is_none() => {
                    cloud = Some(arg == "--deploy-to-cloud");
                }
                "--name" | "--parent" | "--source-rust" | "--config" | "--transport" | "--mode"
                | "--stt" | "--llm" | "--tts" | "--client" | "--eval" => {
                    let value = args.next().filter(|s| !s.starts_with("--")).ok_or(
                        ProjectError::Configuration("option requires a value; use --help"),
                    )?;
                    if values.insert(arg, value).is_some() {
                        return Err(ProjectError::Configuration(
                            "duplicate option; each selection must occur once",
                        ));
                    }
                }
                value if !value.starts_with('-') && !values.contains_key("--name") => {
                    values.insert("--name".into(), value.to_owned());
                }
                _ => {
                    return Err(ProjectError::Configuration(
                        "unknown, duplicate, or unsupported option; use --help",
                    ))
                }
            }
        }
        let mut config = if let Some(path) = values.remove("--config") {
            // Inspect before opening so a FIFO/device cannot block configuration loading.
            if !std::fs::metadata(&path)?.is_file() {
                return Err(ProjectError::Configuration(
                    "config must be a regular JSON file",
                ));
            }
            let file = File::open(path)?;
            if !file.metadata()?.is_file() {
                return Err(ProjectError::Configuration(
                    "config must be a regular JSON file",
                ));
            }
            let mut bytes = Vec::new();
            file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
            ProjectConfig::from_json(&bytes)?
        } else {
            ProjectConfig::default()
        };
        macro_rules! apply {
            ($flag:literal, $field:ident) => {
                if let Some(value) = values.remove($flag) {
                    config.$field = value;
                }
            };
        }
        apply!("--name", name);
        apply!("--transport", transport);
        apply!("--mode", mode);
        apply!("--stt", stt);
        apply!("--llm", llm);
        apply!("--tts", tts);
        apply!("--client", client);
        apply!("--eval", eval);
        if let Some(cloud) = cloud {
            config.deploy_to_cloud = cloud;
        }
        config.validate()?;
        let source_rust = PathBuf::from(values.remove("--source-rust").ok_or(
            ProjectError::Configuration(
                "--source-rust is required; use the existing local Rust workspace",
            ),
        )?);
        let parent = PathBuf::from(values.remove("--parent").unwrap_or_else(|| ".".into()));
        Ok(Self::Generate {
            plan: Box::new(ProjectPlan::new(config, &parent, &source_rust)?),
            dry_run,
        })
    }
}
