//! Deterministic, credential-free scaffolding for the implemented native voice runner.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
pub mod cli;
pub mod config;

pub use config::{supported_options, ProjectConfig};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum ProjectError {
    #[error("configuration: {0}")]
    Configuration(&'static str),
    #[error("destination already exists; choose a new project name (overwriting is disabled)")]
    Exists,
    #[error("generation cancelled")]
    Cancelled,
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration could not be serialized")]
    Serialization,
}

/// Fully validated output in deterministic path order. Creating a plan writes nothing.
/// Its files are private so callers cannot inject arbitrary output paths.
#[derive(Debug, Clone)]
pub struct ProjectPlan {
    target: PathBuf,
    source_rust: PathBuf,
    config: ProjectConfig,
    files: BTreeMap<String, Vec<u8>>,
}
impl ProjectPlan {
    /// `parent` must already exist and contain no `..` or symlink components.
    /// `source_rust` is the existing local workspace containing pipecat-runner.
    pub fn new(
        config: ProjectConfig,
        parent: &Path,
        source_rust: &Path,
    ) -> Result<Self, ProjectError> {
        config.validate()?;
        let parent = checked_parent(parent)?;
        let target = parent.join(&config.name);
        require_absent(&target)?;
        let source_rust = source_rust.canonicalize()?;
        if !source_rust.is_dir()
            || !source_rust.join("Cargo.toml").is_file()
            || !source_rust
                .join("crates/pipecat-runner/Cargo.toml")
                .is_file()
            || !source_rust
                .join("crates/pipecat-runner/src/lib.rs")
                .is_file()
        {
            return Err(ProjectError::Configuration("--source-rust must name a local Rust workspace containing pipecat-runner's library"));
        }
        let runner_literal = dependency_literal(&source_rust, "pipecat-runner")?;
        let (package_options, eval_dependencies, eval_binary, signal_feature) = if config.eval
            == "text"
        {
            let mut dependencies = String::new();
            for name in EVAL_CRATES {
                let literal = dependency_literal(&source_rust, name)?;
                dependencies.push_str(&format!("{name} = {{ path = {literal} }}\n"));
            }
            dependencies.push_str("serde = { version = \"1\", features = [\"derive\"] }\nserde_json = \"1\"\nurl = \"2\"\nreqwest = { version = \"0.12.28\", default-features = false, features = [\"json\", \"stream\", \"rustls-tls\"] }\n");
            (
                format!("default-run = \"{}\"\n", config.name),
                dependencies,
                "[[bin]]\nname = \"eval-bot\"\npath = \"src/bin/eval-bot.rs\"\n\n",
                ", \"signal\"",
            )
        } else {
            (String::new(), String::new(), "", "")
        };
        let webrtc = config.transport == "smallwebrtc";
        let runner_features = if webrtc {
            dependency_literal(&source_rust, "pipecat-webrtc")?;
            for relative in [
                "vendor/webrtc-0.14.0/Cargo.toml",
                "vendor/webrtc-0.14.0/src/lib.rs",
            ] {
                let path = source_rust.join(relative).canonicalize()?;
                if !path.is_file() || !path.starts_with(&source_rust) {
                    return Err(ProjectError::Configuration(
                        "vendored WebRTC source must remain inside the selected workspace",
                    ));
                }
                checked_source_string(&path)?;
            }
            ", features = [\"webrtc\"]"
        } else {
            ""
        };
        let cargo = format!(
            "[package]\nname = \"{}\"\nversion = \"0.1.0\"\nedition = \"2021\"\nrust-version = \"1.99\"\npublish = false\n{package_options}\n# Independent application, even when generated beneath another workspace.\n[workspace]\n\n{eval_binary}[dependencies]\npipecat-runner = {{ path = {}{runner_features} }}\ntokio = {{ version = \"1\", features = [\"rt-multi-thread\", \"macros\"{signal_feature}] }}\n{eval_dependencies}\n[profile.dev]\ndebug = 1\nincremental = false\n\n[lints.rust]\nunsafe_code = \"forbid\"\n",
            config.name, runner_literal
        );
        let mut files = BTreeMap::new();
        files.insert("Cargo.toml".into(), cargo.into_bytes());
        files.insert(
            "src/main.rs".into(),
            include_bytes!("../templates/main.rs").to_vec(),
        );
        if webrtc {
            files.insert(
                "src/lib.rs".into(),
                include_bytes!("../templates/webrtc_lib.rs").to_vec(),
            );
            let main = include_str!("../templates/main.rs").replace(
                "pipecat_runner::run_from_env()",
                &format!("{}::run()", config.name.replace('-', "_")),
            );
            files.insert("src/main.rs".into(), main.into_bytes());
        }
        files.insert(
            ".env.example".into(),
            include_bytes!("../templates/env.example").to_vec(),
        );
        files.insert(
            ".gitignore".into(),
            include_bytes!("../templates/gitignore").to_vec(),
        );
        let client_instructions = if config.client == "rust-wasm" {
            include_client(&mut files, &config.name, &config.transport)?;
            "The included `client/` is a standalone Rust/WASM browser application. Install\nthe WASM target and matching wasm-bindgen tool once, then build its public assets:\n\n```sh\nrustup target add wasm32-unknown-unknown\ncargo install wasm-bindgen-cli --version 0.2.129 --locked\nbash client/build.sh\ncargo run -- --assets client/dist\n```\n\nOpen the printed URL and choose Connect. The server serves the five public build\nassets only; it never serves `.env` or arbitrary project files."
        } else {
            "```sh\ncargo run\n```\n\nThis API-only scaffold has no browser assets. Its `/api/start` accepts a same-origin\nPOST with `{\"transport\":\"websocket\"}` from a compatible RTVI 2.2/protobuf client.\nTo serve the existing Rust browser bundle, build that bundle and pass\n`--assets /absolute/path/to/client/dist`; or generate a new project with\n`--client rust-wasm` for the complete client source. A bare static file server\ndoes not supply the native `/api/start` endpoint."
        };
        let client_instructions =
            client_instructions.replace("\"websocket\"", &format!("\"{}\"", config.transport));
        let mut readme = include_str!("../templates/README.md")
            .replace("{{name}}", &config.name)
            .replace("{{client_instructions}}", &client_instructions);
        if webrtc {
            readme = readme
                .replace(
                    "WebSocket/RTVI 2.2 protobuf audio",
                    "SmallWebRTC encrypted audio + RTVI 2.2",
                )
                .replace("RTVI 2.2/protobuf client", "SmallWebRTC/RTVI 2.2 client")
                .replace(
                    "selected `websocket` transport using native RTVI/protobuf",
                    "selected `smallwebrtc` transport using native encrypted RTC audio and RTVI",
                )
                .replace(
                    "Daily, LiveKit, SmallWebRTC server sessions, telephony",
                    "Daily, LiveKit, telephony",
                );
            readme.push_str(include_str!("../templates/webrtc_README.md"));
        }
        if config.eval == "text" {
            include_eval(&mut files);
            readme.push_str(include_str!("../templates/eval/README.md"));
        }
        files.insert("README.md".into(), readme.into_bytes());
        let mut config_json =
            serde_json::to_vec_pretty(&config).map_err(|_| ProjectError::Serialization)?;
        config_json.push(b'\n');
        files.insert("pipecat-project.json".into(), config_json);
        Ok(Self {
            target,
            source_rust,
            config,
            files,
        })
    }

    pub fn target(&self) -> &Path {
        &self.target
    }
    pub fn source_rust(&self) -> &Path {
        &self.source_rust
    }
    pub fn config(&self) -> &ProjectConfig {
        &self.config
    }
    pub fn files(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
    }
    /// Credential-free dry-run output, suitable for validation before writing.
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "config": self.config, "target": self.target, "source_rust": self.source_rust,
            "files": self.files.keys().collect::<Vec<_>>(), "overwrite": false,
        })
    }
}

const EVAL_CRATES: &[&str] = &[
    "pipecat-core",
    "pipecat-evals",
    "pipecat-processors",
    "pipecat-services",
    "pipecat-turns",
];

fn dependency_literal(source_rust: &Path, name: &str) -> Result<String, ProjectError> {
    let directory = source_rust.join("crates").join(name).canonicalize()?;
    if !directory.is_dir() || !directory.starts_with(source_rust) {
        return Err(ProjectError::Configuration(
            "dependency must be a directory inside the selected local workspace",
        ));
    }
    for relative in ["Cargo.toml", "src/lib.rs"] {
        let file = directory.join(relative).canonicalize()?;
        if !file.is_file() || !file.starts_with(source_rust) {
            return Err(ProjectError::Configuration(
                "dependency manifest and library must remain inside the selected local workspace",
            ));
        }
        checked_source_string(&file)?;
    }
    serde_json::to_string(checked_source_string(&directory)?)
        .map_err(|_| ProjectError::Serialization)
}

fn checked_source_string(path: &Path) -> Result<&str, ProjectError> {
    let value = path.to_str().ok_or(ProjectError::Configuration(
        "local source path must be UTF-8 for Cargo.toml",
    ))?;
    if value.chars().any(char::is_control) {
        return Err(ProjectError::Configuration(
            "local source path must not contain control characters",
        ));
    }
    Ok(value)
}

fn include_eval(files: &mut BTreeMap<String, Vec<u8>>) {
    // Every generated source is embedded explicitly; no checkout files are copied.
    macro_rules! source {
        ($output:literal, $template:literal) => {
            files.insert(
                $output.into(),
                include_bytes!(concat!("../templates/eval/", $template)).to_vec(),
            );
        };
    }
    source!("src/bin/eval-bot.rs", "eval-bot.rs");
    source!("src/eval_app.rs", "eval_app.rs");
    source!("evals/starter_text.yaml", "starter_text.yaml");
    source!("evals/bot.json", "bot.json");
    let manifest = include_str!("../templates/eval/native-manifest.yaml")
        .replace("{{exe_suffix}}", std::env::consts::EXE_SUFFIX);
    files.insert("evals/native-manifest.yaml".into(), manifest.into_bytes());
}

fn include_client(
    files: &mut BTreeMap<String, Vec<u8>>,
    name: &str,
    transport: &str,
) -> Result<(), ProjectError> {
    // Only reviewed source files are embedded. Never recursively copy a working
    // checkout, .env files, cached builds, credentials, or downloaded artifacts.
    macro_rules! source {
        ($path:literal) => {
            files.insert(
                concat!("client/", $path).into(),
                include_bytes!(concat!("../../../client-templates/", $path)).to_vec(),
            );
        };
    }
    source!("Cargo.toml");
    source!("Cargo.lock");
    source!("build.sh");
    source!(".gitignore");
    source!("src/lib.rs");
    source!("src/lifecycle.rs");
    source!("src/protocol.rs");
    source!("src/browser.rs");
    source!("web/index.html");
    source!("web/style.css");
    let mut config = serde_json::to_vec_pretty(&serde_json::json!({
        "project_name":name,"start_endpoint":"/api/start","transports":[transport]
    }))
    .map_err(|_| ProjectError::Serialization)?;
    config.push(b'\n');
    files.insert("client/web/config.json".into(), config);
    Ok(())
}

fn require_absent(path: &Path) -> Result<(), ProjectError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(ProjectError::Exists),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}
fn checked_parent(parent: &Path) -> Result<PathBuf, ProjectError> {
    if parent
        .to_str()
        .is_none_or(|text| text.chars().any(char::is_control))
    {
        return Err(ProjectError::Configuration(
            "parent path must be UTF-8 without control characters",
        ));
    }
    if parent.as_os_str().is_empty()
        || parent
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(ProjectError::Configuration(
            "parent must be an existing directory without '..' components",
        ));
    }
    let absolute = if parent.is_absolute() {
        parent.to_owned()
    } else {
        std::env::current_dir()?.join(parent)
    };
    let mut prefix = PathBuf::new();
    for component in absolute.components() {
        prefix.push(component);
        let metadata = fs::symlink_metadata(&prefix)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ProjectError::Configuration(
                "parent path must contain only existing directories, not symlinks",
            ));
        }
    }
    Ok(absolute.canonicalize()?)
}

/// Progress reported before each file and once before committing the result.
#[derive(Debug, Copy, Clone)]
pub struct Progress {
    pub completed_files: usize,
    pub total_files: usize,
}

/// Creates a new project. Existing files, directories, and dangling symlinks all
/// fail closed; there is deliberately no overwrite option.
pub fn generate(plan: &ProjectPlan) -> Result<PathBuf, ProjectError> {
    generate_cancellable(plan, |_| false)
}

/// A `true` cancellation callback result rolls back this invocation's files.
/// Ordinary errors and unwinding panics use the same rollback guard. The parent
/// directory must be trusted: this is a project generator, not a filesystem sandbox.
/// SIGKILL/power loss cannot run cleanup; such interruptions can leave partial files.
pub fn generate_cancellable(
    plan: &ProjectPlan,
    mut is_cancelled: impl FnMut(Progress) -> bool,
) -> Result<PathBuf, ProjectError> {
    let total_files = plan.files.len();
    if is_cancelled(Progress {
        completed_files: 0,
        total_files,
    }) {
        return Err(ProjectError::Cancelled);
    }
    let parent = plan
        .target
        .parent()
        .ok_or(ProjectError::Configuration("missing parent directory"))?;
    checked_parent(parent)?;
    // create_dir itself is the no-overwrite check, including races after planning.
    fs::create_dir(&plan.target).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            ProjectError::Exists
        } else {
            error.into()
        }
    })?;
    let mut rollback = Rollback {
        files: Vec::new(),
        directories: vec![plan.target.clone()],
        committed: false,
    };
    for (completed_files, (relative, bytes)) in plan.files.iter().enumerate() {
        if is_cancelled(Progress {
            completed_files,
            total_files,
        }) {
            return Err(ProjectError::Cancelled);
        }
        let relative = Path::new(relative);
        if relative.is_absolute()
            || relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(ProjectError::Configuration(
                "template output path is invalid",
            ));
        }
        let mut directory = plan.target.clone();
        if let Some(parents) = relative.parent() {
            for component in parents.components() {
                directory.push(component);
                if rollback.directories.contains(&directory) {
                    if fs::symlink_metadata(&directory)?.file_type().is_symlink() {
                        return Err(ProjectError::Configuration(
                            "output directory became a symlink",
                        ));
                    }
                    continue;
                }
                fs::create_dir(&directory)?;
                rollback.directories.push(directory.clone());
            }
        }
        let destination = plan.target.join(relative);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)?;
        rollback.files.push(destination);
        file.write_all(bytes)?;
        file.flush()?;
    }
    if is_cancelled(Progress {
        completed_files: total_files,
        total_files,
    }) {
        return Err(ProjectError::Cancelled);
    }
    rollback.committed = true;
    Ok(plan.target.clone())
}

struct Rollback {
    files: Vec<PathBuf>,
    directories: Vec<PathBuf>,
    committed: bool,
}
impl Drop for Rollback {
    fn drop(&mut self) {
        if !self.committed {
            for file in self.files.iter().rev() {
                let _ = fs::remove_file(file);
            }
            // Never recursively remove a directory: unrelated files created by
            // somebody else while generation ran must be retained.
            for directory in self.directories.iter().rev() {
                let _ = fs::remove_dir(directory);
            }
        }
    }
}

#[cfg(test)]
mod tests;
