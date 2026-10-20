//! Process-level checks; generation never calls Python or external package installers.
use std::{fs, path::PathBuf, process::Command};

fn source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}
fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pipecat-init"))
}

#[test]
fn dry_run_then_generation_and_repeat_have_correct_exit_status() {
    let temp = tempfile::tempdir().unwrap();
    let mut args = vec![
        "example-bot".to_owned(),
        "--source-rust".into(),
        source().display().to_string(),
        "--parent".into(),
        temp.path().display().to_string(),
    ];
    args.push("--dry-run".into());
    let output = command().args(&args).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(summary["config"]["name"], "example-bot");
    assert!(!temp.path().join("example-bot").exists());
    args.pop();
    assert!(command().args(&args).output().unwrap().status.success());
    let output = command().args(&args).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("destination already exists"));
    assert!(temp.path().join("example-bot/src/main.rs").is_file());
}

#[test]
fn unsupported_and_malicious_input_does_not_create_anything() {
    let temp = tempfile::tempdir().unwrap();
    for args in [
        vec!["../escape"],
        vec!["safe-bot", "--transport", "daily"],
        vec!["safe-bot", "--transport", "livekit"],
        vec!["safe-bot", "--deploy-to-cloud"],
    ] {
        let output = command()
            .args(args)
            .arg("--parent")
            .arg(temp.path())
            .arg("--source-rust")
            .arg(source())
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }
}

#[test]
fn process_environment_secrets_never_appear_in_generated_files() {
    let temp = tempfile::tempdir().unwrap();
    let output = command()
        .args([
            "secret-free",
            "--client",
            "rust-wasm",
            "--eval",
            "text",
            "--parent",
        ])
        .arg(temp.path())
        .arg("--source-rust")
        .arg(source())
        .env("DEEPGRAM_API_KEY", "SHOULD_NEVER_BE_COPIED_18A6")
        .env("OPENAI_API_KEY", "SHOULD_NEVER_BE_COPIED_18A6")
        .env("CARTESIA_API_KEY", "SHOULD_NEVER_BE_COPIED_18A6")
        .output()
        .unwrap();
    assert!(output.status.success());
    let mut pending = vec![temp.path().to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let content = fs::read_to_string(&path).unwrap();
                assert!(
                    !content.contains("SHOULD_NEVER_BE_COPIED_18A6"),
                    "secret copied to {}",
                    path.display()
                );
                assert_ne!(path.file_name().unwrap(), ".env");
            }
        }
    }
}

#[test]
#[ignore = "compiles a complete generated native application using the cached Cargo registry"]
fn generated_application_builds_and_runs_without_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let config = pipecat_project::ProjectConfig::new("generated-check");
    let plan = pipecat_project::ProjectPlan::new(config, temp.path(), &source()).unwrap();
    let project = pipecat_project::generate(&plan).unwrap();
    let output = Command::new(env!("CARGO"))
        .args(["run", "--offline", "--manifest-path"])
        .arg(project.join("Cargo.toml"))
        .arg("--target-dir")
        // This generated workspace must not reuse another source tree's Cargo
        // artifacts. Its temporary directory owns the build cache as well.
        .arg(project.join("target"))
        .args(["--", "--help"])
        .env_remove("DEEPGRAM_API_KEY")
        .env_remove("OPENAI_API_KEY")
        .env_remove("CARTESIA_API_KEY")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Native HTTP /api/start"));
    assert!(project.join("Cargo.lock").exists());
}

#[test]
fn smallwebrtc_cli_dry_run_generation_and_no_clobber() {
    let temp = tempfile::tempdir().unwrap();
    let args = [
        "rtc-bot",
        "--transport",
        "smallwebrtc",
        "--client",
        "rust-wasm",
    ];
    let invoke = |dry: bool| {
        let mut cmd = command();
        cmd.args(args)
            .arg("--parent")
            .arg(temp.path())
            .arg("--source-rust")
            .arg(source());
        if dry {
            cmd.arg("--dry-run");
        }
        cmd.output().unwrap()
    };
    let dry = invoke(true);
    assert!(
        dry.status.success(),
        "{}",
        String::from_utf8_lossy(&dry.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&dry.stdout).unwrap();
    assert_eq!(summary["config"]["transport"], "smallwebrtc");
    assert!(!temp.path().join("rtc-bot").exists());
    assert!(invoke(false).status.success());
    assert!(!invoke(false).status.success());
    let config: serde_json::Value = serde_json::from_slice(
        &fs::read(temp.path().join("rtc-bot/client/web/config.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(config["transports"], serde_json::json!(["smallwebrtc"]));
}

#[test]
fn twilio_cli_options_generation_and_secret_free_environment() {
    let temp = tempfile::tempdir().unwrap();
    let options = command().arg("--list-options").output().unwrap();
    assert!(options.status.success());
    let options: serde_json::Value = serde_json::from_slice(&options.stdout).unwrap();
    assert!(options["transport"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("twilio")));
    let invoke = |extra: &[&str]| {
        command()
            .args(["phone-bot", "--transport", "twilio"])
            .args(extra)
            .arg("--parent")
            .arg(temp.path())
            .arg("--source-rust")
            .arg(source())
            .env("TWILIO_AUTH_TOKEN", "SYNTHETIC_GENERATOR_NOT_COPIED_147")
            .env("TWILIO_ACCOUNT_SID", "SYNTHETIC_GENERATOR_NOT_COPIED_147")
            .output()
            .unwrap()
    };
    for rejected in [["--client", "rust-wasm"], ["--eval", "text"]] {
        let output = invoke(&rejected);
        assert!(!output.status.success());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }
    let dry = invoke(&["--dry-run"]);
    assert!(dry.status.success());
    let summary: serde_json::Value = serde_json::from_slice(&dry.stdout).unwrap();
    assert_eq!(summary["config"]["transport"], "twilio");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    assert!(invoke(&[]).status.success());
    assert!(!invoke(&[]).status.success());
    let project = temp.path().join("phone-bot");
    assert!(project.join("src/lib.rs").is_file());
    assert!(!project.join("client").exists());
    assert!(!project.join("evals").exists());
    for path in [
        "Cargo.toml",
        "README.md",
        ".env.example",
        "src/main.rs",
        "src/lib.rs",
        "pipecat-project.json",
    ] {
        assert!(!fs::read_to_string(project.join(path))
            .unwrap()
            .contains("SYNTHETIC_GENERATOR_NOT_COPIED_147"));
    }
}
