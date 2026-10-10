use super::*;
use crate::{cli::Command, config::validate_name};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn plan(temp: &tempfile::TempDir, name: &str) -> ProjectPlan {
    ProjectPlan::new(ProjectConfig::new(name), temp.path(), &source()).unwrap()
}
fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).into()).collect()
}

#[test]
fn names_reject_traversal_injection_and_reserved_identifiers() {
    for name in [
        "",
        ".",
        "..",
        "../outside",
        "/absolute",
        "a/b",
        "a\\b",
        "hello world",
        "a\n[dependencies]",
        "a\0b",
        "Ünicode",
        "1bad",
        "Upper",
        "a.",
        "a-",
        "a_",
        "fn",
        "async",
        "gen",
        "con",
        "com1",
        "pipecat-runner",
        "tokio",
    ] {
        assert!(validate_name(name).is_err(), "accepted {name:?}");
    }
    assert!(validate_name(&"a".repeat(65)).is_err());
    for name in ["a", "voice-bot", "voice_bot2", &"a".repeat(64)] {
        validate_name(name).unwrap();
    }
}
#[test]
fn unsupported_integrations_are_explicit_errors() {
    for transport in ["daily", "livekit", "smallwebrtc", "twilio", "eval"] {
        let mut config = ProjectConfig::new("test-bot");
        config.transport = transport.into();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("only websocket"));
    }
    let mut config = ProjectConfig::new("test-bot");
    config.deploy_to_cloud = true;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("cloud deployment"));
    config.deploy_to_cloud = false;
    config.mode = "realtime".into();
    assert!(config.validate().is_err());
    config.mode = "cascade".into();
    config.llm = "anthropic".into();
    assert!(config.validate().is_err());
    config.llm = "openai".into();
    config.client = "react".into();
    assert!(config.validate().is_err());
}
#[test]
fn strict_config_rejects_secrets_unknown_fields_duplicates_and_oversize() {
    for bytes in [
        br#"{"name":"bot","OPENAI_API_KEY":"SECRET"}"#.as_slice(),
        br#"{"name":"bot","name":"other"}"#,
        br#"{"name":"bot","deploy_to_cloud":"false"}"#,
        b"[]",
        b"null",
    ] {
        let error = ProjectConfig::from_json(bytes).unwrap_err().to_string();
        assert!(!error.contains("SECRET"));
    }
    assert!(ProjectConfig::from_json(&vec![b' '; 16 * 1024 + 1]).is_err());
    let config = ProjectConfig::from_json(br#"{"name":"test-bot","client":"rust-wasm"}"#).unwrap();
    assert_eq!(config.transport, "websocket");
    config.validate().unwrap();
}
#[test]
fn plans_are_deterministic_and_do_not_write() {
    let temp = tempfile::tempdir().unwrap();
    let first = plan(&temp, "voice-bot");
    let second = plan(&temp, "voice-bot");
    assert_eq!(first.files, second.files);
    assert!(!first.target.exists());
    let manifest = std::str::from_utf8(first.files.get("Cargo.toml").unwrap()).unwrap();
    assert!(manifest.contains(&source().canonicalize().unwrap().display().to_string()));
    assert!(!manifest.contains("git ="));
    assert!(!manifest.contains("pipecat-ai"));
}
#[test]
fn generates_expected_files_and_rejects_second_run() {
    let temp = tempfile::tempdir().unwrap();
    let plan = plan(&temp, "voice-bot");
    let root = generate(&plan).unwrap();
    assert_eq!(
        fs::read(root.join("src/main.rs")).unwrap(),
        plan.files["src/main.rs"]
    );
    assert!(root.join(".env.example").exists());
    assert!(!root.join(".env").exists());
    assert!(matches!(generate(&plan), Err(ProjectError::Exists)));
    assert!(matches!(
        ProjectPlan::new(ProjectConfig::new("voice-bot"), temp.path(), &source()),
        Err(ProjectError::Exists)
    ));
}
#[test]
fn existing_empty_directory_and_file_are_never_overwritten() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("empty-bot")).unwrap();
    fs::write(temp.path().join("file-bot"), "keep").unwrap();
    for name in ["empty-bot", "file-bot"] {
        assert!(matches!(
            ProjectPlan::new(ProjectConfig::new(name), temp.path(), &source()),
            Err(ProjectError::Exists)
        ));
    }
    assert_eq!(
        fs::read_to_string(temp.path().join("file-bot")).unwrap(),
        "keep"
    );
}
#[test]
fn destination_created_after_planning_is_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let plan = plan(&temp, "voice-bot");
    fs::create_dir(plan.target()).unwrap();
    fs::write(plan.target().join("keep.txt"), "keep").unwrap();
    assert!(matches!(generate(&plan), Err(ProjectError::Exists)));
    assert_eq!(
        fs::read_to_string(plan.target().join("keep.txt")).unwrap(),
        "keep"
    );
}
#[test]
fn invalid_parent_and_missing_source_leave_no_files() {
    let temp = tempfile::tempdir().unwrap();
    assert!(ProjectPlan::new(
        ProjectConfig::new("voice-bot"),
        &temp.path().join(".."),
        &source()
    )
    .is_err());
    assert!(ProjectPlan::new(
        ProjectConfig::new("voice-bot"),
        &temp.path().join("missing"),
        &source()
    )
    .is_err());
    assert!(ProjectPlan::new(ProjectConfig::new("voice-bot"), temp.path(), temp.path()).is_err());
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}
#[test]
fn cancellation_before_during_and_after_writes_removes_owned_output() {
    let temp = tempfile::tempdir().unwrap();
    let plan = plan(&temp, "voice-bot");
    for cancel_at in 0..=plan.files.len() {
        assert!(matches!(
            generate_cancellable(&plan, |progress| progress.completed_files == cancel_at),
            Err(ProjectError::Cancelled)
        ));
        assert!(!plan.target.exists(), "left output at {cancel_at}");
    }
}
#[test]
fn io_failure_and_panic_roll_back_partial_output() {
    let temp = tempfile::tempdir().unwrap();
    let mut broken = plan(&temp, "voice-bot");
    // This file conflicts with the later source directory and produces a real I/O error.
    broken.files.insert("src".into(), b"conflict".to_vec());
    assert!(matches!(generate(&broken), Err(ProjectError::Io(_))));
    assert!(!broken.target.exists());
    let normal = plan(&temp, "voice-bot");
    let result = catch_unwind(AssertUnwindSafe(|| {
        generate_cancellable(&normal, |progress| {
            if progress.completed_files == 2 {
                panic!("test interruption")
            };
            false
        })
    }));
    assert!(result.is_err());
    assert!(!normal.target.exists());
}
#[test]
fn cancellation_preserves_unrelated_files_added_during_generation() {
    let temp = tempfile::tempdir().unwrap();
    let plan = plan(&temp, "voice-bot");
    assert!(matches!(
        generate_cancellable(&plan, |progress| {
            if progress.completed_files == 1 {
                fs::write(plan.target.join("keep.txt"), "keep").unwrap();
                true
            } else {
                false
            }
        }),
        Err(ProjectError::Cancelled)
    ));
    assert_eq!(
        fs::read_to_string(plan.target.join("keep.txt")).unwrap(),
        "keep"
    );
    assert_eq!(fs::read_dir(&plan.target).unwrap().count(), 1);
}
#[test]
fn client_source_is_allowlisted_and_websocket_only() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = ProjectConfig::new("voice-bot");
    config.client = "rust-wasm".into();
    let plan = ProjectPlan::new(config, temp.path(), &source()).unwrap();
    let paths = plan
        .files
        .keys()
        .filter(|s| s.starts_with("client/"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 11);
    assert!(!paths.iter().any(|s| s.contains("dist/")
        || s.contains("target/")
        || s.contains(".env")
        || s.ends_with(".py")));
    let config: serde_json::Value =
        serde_json::from_slice(&plan.files["client/web/config.json"]).unwrap();
    assert_eq!(config["transports"], serde_json::json!(["websocket"]));
    assert_eq!(config["project_name"], "voice-bot");
    generate(&plan).unwrap();
    assert!(plan.target.join("client/src/browser.rs").exists());
}
#[test]
fn cli_help_options_and_errors_are_side_effect_free() {
    assert!(matches!(
        Command::parse(strings(&["--help"])).unwrap(),
        Command::Help
    ));
    assert!(matches!(
        Command::parse(strings(&["--list-options"])).unwrap(),
        Command::ListOptions
    ));
    for args in [
        &["--name"][..],
        &["--name", "a", "--name", "b"],
        &["--wat"],
        &["--overwrite"],
        &["a", "--deploy-to-cloud"],
        &["--name", "a", "--source-rust"],
    ] {
        assert!(Command::parse(strings(args)).is_err());
    }
}
#[test]
fn cli_config_overrides_are_order_independent_and_dry_run_writes_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("config.json");
    fs::write(
        &file,
        r#"{"name":"from-file","client":"none","deploy_to_cloud":true}"#,
    )
    .unwrap();
    for config_first in [false, true] {
        let mut args = strings(&[
            "--name",
            "from-cli",
            "--client",
            "rust-wasm",
            "--no-deploy-to-cloud",
            "--dry-run",
            "--parent",
        ]);
        args.push(temp.path().display().to_string());
        args.extend(strings(&["--source-rust"]));
        args.push(source().display().to_string());
        let pair = vec!["--config".into(), file.display().to_string()];
        if config_first {
            args.splice(0..0, pair);
        } else {
            args.extend(pair);
        }
        let Command::Generate { plan, dry_run } = Command::parse(args).unwrap() else {
            panic!("wrong command")
        };
        assert!(dry_run);
        assert_eq!(plan.config.name, "from-cli");
        assert_eq!(plan.config.client, "rust-wasm");
        assert!(!plan.config.deploy_to_cloud);
        assert!(!plan.target.exists());
    }
}
#[cfg(unix)]
#[test]
fn symlinks_and_dangling_destinations_are_not_followed() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), temp.path().join("link")).unwrap();
    assert!(ProjectPlan::new(
        ProjectConfig::new("voice-bot"),
        &temp.path().join("link"),
        &source()
    )
    .is_err());
    symlink(
        outside.path().join("missing"),
        temp.path().join("voice-bot"),
    )
    .unwrap();
    assert!(matches!(
        ProjectPlan::new(ProjectConfig::new("voice-bot"), temp.path(), &source()),
        Err(ProjectError::Exists)
    ));
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
}
#[cfg(unix)]
#[test]
fn dependency_path_is_escaped_for_toml() {
    let temp = tempfile::tempdir().unwrap();
    let unusual = temp.path().join("source-\"quoted\"-\\slash");
    fs::create_dir_all(unusual.join("crates/pipecat-runner/src")).unwrap();
    fs::write(unusual.join("Cargo.toml"), "[workspace]").unwrap();
    fs::write(
        unusual.join("crates/pipecat-runner/Cargo.toml"),
        "[package]",
    )
    .unwrap();
    fs::write(unusual.join("crates/pipecat-runner/src/lib.rs"), "").unwrap();
    let plan = ProjectPlan::new(ProjectConfig::new("voice-bot"), temp.path(), &unusual).unwrap();
    let manifest = std::str::from_utf8(&plan.files["Cargo.toml"]).unwrap();
    assert!(manifest.contains("\\\"quoted\\\""));
    assert!(manifest.contains("\\\\slash"));
}

#[cfg(unix)]
#[test]
fn non_unicode_and_control_character_parents_are_rejected() {
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().unwrap();
    for suffix in [
        std::ffi::OsString::from_vec(vec![b'a', 0xff]),
        std::ffi::OsString::from("line\nbreak"),
    ] {
        let path = temp.path().join(suffix);
        fs::create_dir(&path).unwrap();
        assert!(ProjectPlan::new(ProjectConfig::new("voice-bot"), &path, &source()).is_err());
    }
}

#[cfg(unix)]
#[test]
fn config_fifo_is_rejected_before_opening() {
    let temp = tempfile::tempdir().unwrap();
    let fifo = temp.path().join("config-fifo");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(status.success());
    let error = Command::parse(vec!["--config".into(), fifo.display().to_string()]).unwrap_err();
    assert!(error.to_string().contains("regular JSON file"));
}
