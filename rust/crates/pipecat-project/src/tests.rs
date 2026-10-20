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
    for transport in [
        "daily",
        "livekit",
        "webrtc",
        "SmallWebRTC",
        "twilio",
        "eval",
    ] {
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

fn eval_plan(temp: &tempfile::TempDir, client: &str) -> ProjectPlan {
    let mut config = ProjectConfig::new("eval-starter");
    config.eval = "text".into();
    config.client = client.into();
    ProjectPlan::new(config, temp.path(), &source()).unwrap()
}

#[test]
fn eval_selection_is_strict_optional_and_reserves_only_introduced_collisions() {
    let config = ProjectConfig::from_json(br#"{"name":"bot"}"#).unwrap();
    assert_eq!(config.eval, "none");
    assert_eq!(
        supported_options()["eval"],
        serde_json::json!(["none", "text"])
    );
    for selection in ["audio", "realtime", "Text", "", "python", "none,text"] {
        let mut config = ProjectConfig::new("bot");
        config.eval = selection.into();
        assert!(config.validate().is_err());
    }
    for bytes in [
        br#"{"name":"bot","eval":true}"#.as_slice(),
        br#"{"name":"bot","eval":"text","eval":"none"}"#,
        br#"{"name":"bot","enable_eval":true}"#,
        br#"{"name":"bot","eval":"text","llm_url":"SECRET"}"#,
    ] {
        let error = ProjectConfig::from_json(bytes).unwrap_err().to_string();
        assert!(!error.contains("SECRET"));
    }
    for name in [
        "eval-bot",
        "eval_bot",
        "pipecat-core",
        "pipecat_evals",
        "pipecat-processors",
        "pipecat_services",
        "pipecat-turns",
        "reqwest",
        "serde",
        "serde-json",
        "serde_json",
        "url",
    ] {
        let mut config = ProjectConfig::new(name);
        config.validate().unwrap();
        config.eval = "text".into();
        assert!(config.validate().is_err(), "accepted collision {name}");
    }
}

#[test]
fn eval_plan_has_exact_allowlist_and_preserves_default_voice_and_client() {
    const EXTRA: &[&str] = &[
        "evals/bot.json",
        "evals/native-manifest.yaml",
        "evals/starter_text.yaml",
        "src/bin/eval-bot.rs",
        "src/eval_app.rs",
    ];
    for client in ["none", "rust-wasm"] {
        let temp = tempfile::tempdir().unwrap();
        let mut config = ProjectConfig::new("eval-starter");
        config.client = client.into();
        let plain = ProjectPlan::new(config, temp.path(), &source()).unwrap();
        let eval = eval_plan(&temp, client);
        assert_eq!(eval.files, eval_plan(&temp, client).files);
        assert_eq!(plain.files.len() + EXTRA.len(), eval.files.len());
        let extra = eval
            .files
            .keys()
            .filter(|key| !plain.files.contains_key(*key))
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(extra, EXTRA);
        for (path, bytes) in &plain.files {
            if !["Cargo.toml", "README.md", "pipecat-project.json"].contains(&path.as_str()) {
                assert_eq!(bytes, &eval.files[path], "unexpected change: {path}");
            }
        }
        let plain_cargo = std::str::from_utf8(&plain.files["Cargo.toml"]).unwrap();
        assert!(!plain_cargo.contains("pipecat-evals"));
        assert!(!plain_cargo.contains("eval-bot"));
        let cargo = std::str::from_utf8(&eval.files["Cargo.toml"]).unwrap();
        assert!(cargo.contains("default-run = \"eval-starter\""));
        assert!(cargo.contains("name = \"eval-bot\""));
        assert!(!cargo.contains("native-bot-example"));
        assert!(!cargo.contains("process-fixtures"));
        assert!(!cargo.contains("git ="));
        for dependency in EVAL_CRATES {
            assert!(cargo.contains(&format!("{dependency} = {{ path = ")));
        }
        let entry = std::str::from_utf8(&eval.files["src/bin/eval-bot.rs"]).unwrap();
        assert!(entry.contains("#[path = \"../eval_app.rs\"]"));
        assert!(!entry.contains("examples/"));
        let choices: serde_json::Value =
            serde_json::from_slice(&eval.files["pipecat-project.json"]).unwrap();
        assert_eq!(choices["eval"], "text");
        assert!(!eval.target.exists());
    }
}

#[test]
fn generated_eval_scenario_and_manifest_use_existing_native_loaders() {
    let temp = tempfile::tempdir().unwrap();
    let plan = eval_plan(&temp, "none");
    let root = generate(&plan).unwrap();
    let file = pipecat_evals::ScenarioFile::load(root.join("evals/starter_text.yaml")).unwrap();
    assert_eq!(file.name, "starter_text");
    assert_eq!(file.scenarios.len(), 1);
    let scenario = &file.scenarios[0];
    assert_eq!(scenario.name, "starter_text/two_turn_tool");
    assert_eq!(scenario.turns.len(), 2);
    assert!(scenario.turns.iter().all(|turn| turn.user.is_some()));
    assert!(!scenario.needs_judge());
    assert!(scenario.judge.as_ref().unwrap().eval.is_none());
    assert_eq!(scenario.turns[0].expect[0].event, "function_call");
    let call = &scenario.turns[0].expect[0].calls.as_ref().unwrap()[0];
    assert_eq!(call.name.as_deref(), Some("add"));
    assert_eq!(
        call.args.as_ref().unwrap().get("a"),
        Some(&serde_json::json!(2))
    );
    assert_eq!(
        call.args.as_ref().unwrap().get("b"),
        Some(&serde_json::json!(3))
    );
    assert_eq!(scenario.turns[0].expect[1].event, "llm_response");
    assert_eq!(
        scenario.turns[1].expect[0].text_contains.as_deref(),
        Some("5")
    );
    let manifest =
        pipecat_evals::release::ReleaseManifest::load(root.join("evals/native-manifest.yaml"))
            .unwrap();
    assert_eq!(manifest.concurrency, 1);
    assert_eq!(manifest.jobs.len(), 1);
    let job = &manifest.jobs[0];
    assert_eq!(job.scenario.name, scenario.name);
    assert_eq!(job.name, "native-text-starter");
    assert_eq!(job.port, 7900);
    assert_eq!(job.attempt, 1);
    assert_eq!(
        job.bot,
        root.join("evals/../target/debug")
            .join(format!("eval-bot{}", std::env::consts::EXE_SUFFIX))
    );
    let config: serde_json::Value = serde_json::from_slice(&plan.files["evals/bot.json"]).unwrap();
    assert_eq!(
        config,
        serde_json::json!({"native_text_bot_version":1,"llm_url":"","model":""})
    );
    // The manifest validates mapping shape; the bot validates model semantics before bind.
    assert!(!job.bot.exists());
}

#[test]
fn eval_cli_precedence_and_invalid_flags_do_not_write() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("config.json");
    fs::write(&file, r#"{"name":"eval-starter","eval":"text"}"#).unwrap();
    for config_first in [true, false] {
        for eval in ["none", "text"] {
            let mut args = strings(&["--eval", eval, "--dry-run", "--source-rust"]);
            args.push(source().display().to_string());
            args.extend(strings(&["--parent"]));
            args.push(temp.path().display().to_string());
            let config = vec!["--config".into(), file.display().to_string()];
            if config_first {
                args.splice(0..0, config);
            } else {
                args.extend(config);
            }
            let Command::Generate { plan, dry_run } = Command::parse(args).unwrap() else {
                panic!("expected plan");
            };
            assert!(dry_run);
            assert_eq!(plan.config.eval, eval);
            assert_eq!(plan.files.contains_key("evals/bot.json"), eval == "text");
            assert!(!plan.target.exists());
        }
    }
    for args in [
        vec!["--eval"],
        vec!["--eval", "text", "--eval", "none"],
        vec!["safe-bot", "--eval", "audio"],
        vec!["safe-bot", "--enable-eval"],
    ] {
        assert!(Command::parse(strings(&args)).is_err());
    }
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    assert!(crate::cli::HELP.contains("--eval none|text"));
}

#[test]
fn eval_cancellation_rolls_back_every_boundary_including_final_commit() {
    for client in ["none", "rust-wasm"] {
        let temp = tempfile::tempdir().unwrap();
        let plan = eval_plan(&temp, client);
        // Initial pre-create checkpoint, then every file and final commit checkpoint.
        for cancel_call in 0..plan.files.len() + 2 {
            let mut calls = 0;
            let result = generate_cancellable(&plan, |_| {
                let cancel = calls == cancel_call;
                calls += 1;
                cancel
            });
            assert!(matches!(result, Err(ProjectError::Cancelled)));
            assert!(!plan.target.exists(), "residue at callback {cancel_call}");
        }
    }
}

#[test]
fn eval_io_failure_panic_and_nested_unrelated_files_use_owned_rollback() {
    let temp = tempfile::tempdir().unwrap();
    let plan = eval_plan(&temp, "none");
    let mut broken = plan.clone();
    broken
        .files
        .insert("evals/conflict".into(), b"file".to_vec());
    broken
        .files
        .insert("evals/conflict/file".into(), b"bad".to_vec());
    assert!(matches!(generate(&broken), Err(ProjectError::Io(_))));
    assert!(!plan.target.exists());
    let result = catch_unwind(AssertUnwindSafe(|| {
        generate_cancellable(&plan, |progress| {
            if progress.completed_files == progress.total_files {
                panic!("after eval writes");
            }
            false
        })
    }));
    assert!(result.is_err());
    assert!(!plan.target.exists());
    let result = generate_cancellable(&plan, |progress| {
        if progress.completed_files == progress.total_files {
            fs::write(plan.target.join("evals/keep.txt"), "unrelated").unwrap();
            true
        } else {
            false
        }
    });
    assert!(matches!(result, Err(ProjectError::Cancelled)));
    assert_eq!(
        fs::read_to_string(plan.target.join("evals/keep.txt")).unwrap(),
        "unrelated"
    );
    assert_eq!(fs::read_dir(plan.target.join("evals")).unwrap().count(), 1);
    assert_eq!(fs::read_dir(plan.target()).unwrap().count(), 1);
}

fn make_source_fixture(path: &Path) {
    fs::create_dir_all(path).unwrap();
    fs::write(path.join("Cargo.toml"), "[workspace]").unwrap();
    for name in std::iter::once("pipecat-runner").chain(EVAL_CRATES.iter().copied()) {
        let crate_path = path.join("crates").join(name);
        fs::create_dir_all(crate_path.join("src")).unwrap();
        fs::write(crate_path.join("Cargo.toml"), "[package]").unwrap();
        fs::write(crate_path.join("src/lib.rs"), "").unwrap();
    }
}

#[test]
fn every_eval_dependency_is_validated_before_creating_target() {
    for name in EVAL_CRATES {
        for missing in ["Cargo.toml", "src/lib.rs"] {
            let temp = tempfile::tempdir().unwrap();
            let source = temp.path().join("source");
            make_source_fixture(&source);
            fs::remove_file(source.join("crates").join(name).join(missing)).unwrap();
            let mut config = ProjectConfig::new("eval-starter");
            config.eval = "text".into();
            assert!(ProjectPlan::new(config, temp.path(), &source).is_err());
            assert!(!temp.path().join("eval-starter").exists());
            // Unselected eval crates must not become requirements for the default.
            ProjectPlan::new(ProjectConfig::new("voice-bot"), temp.path(), &source).unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn all_eval_paths_escape_toml_and_reject_source_symlink_escapes() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source space-\"quote\"-\\slash");
    make_source_fixture(&source);
    let mut config = ProjectConfig::new("eval-starter");
    config.eval = "text".into();
    let plan = ProjectPlan::new(config.clone(), temp.path(), &source).unwrap();
    let cargo = std::str::from_utf8(&plan.files["Cargo.toml"]).unwrap();
    for line in cargo
        .lines()
        .filter(|line| line.contains("path = ") && line.contains("crates"))
    {
        assert!(
            line.contains("source space-\\\"quote\\\"-\\\\slash"),
            "unescaped dependency: {line}"
        );
    }
    for name in EVAL_CRATES {
        for component in ["Cargo.toml", "src/lib.rs"] {
            let inside = source.join("crates").join(name).join(component);
            let outside = temp.path().join("outside");
            fs::write(&outside, "keep").unwrap();
            fs::remove_file(&inside).unwrap();
            symlink(&outside, &inside).unwrap();
            assert!(ProjectPlan::new(config.clone(), temp.path(), &source).is_err());
            assert!(!plan.target.exists());
            fs::remove_file(&inside).unwrap();
            fs::write(&inside, "").unwrap();
        }
    }
    let escaped = source.join("crates/pipecat-evals");
    let moved = temp.path().join("moved-evals");
    fs::rename(&escaped, &moved).unwrap();
    symlink(&moved, &escaped).unwrap();
    assert!(ProjectPlan::new(config, temp.path(), &source).is_err());
    assert!(!plan.target.exists());
}

#[cfg(unix)]
#[test]
fn eval_source_paths_reject_non_unicode_and_controls_before_writes() {
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().unwrap();
    for suffix in [
        std::ffi::OsString::from_vec(vec![b's', 0xff]),
        std::ffi::OsString::from("source\ncontrol"),
    ] {
        let source = temp.path().join(suffix);
        make_source_fixture(&source);
        let mut config = ProjectConfig::new("eval-starter");
        config.eval = "text".into();
        assert!(ProjectPlan::new(config, temp.path(), &source).is_err());
        assert!(!temp.path().join("eval-starter").exists());
    }
}

#[test]
fn smallwebrtc_plan_selects_real_runtime_client_and_shared_constructor() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = ProjectConfig::new("rtc-example");
    config.transport = "smallwebrtc".into();
    config.client = "rust-wasm".into();
    config.eval = "text".into();
    let plan = ProjectPlan::new(config, temp.path(), &source()).unwrap();
    let text = |name: &str| std::str::from_utf8(&plan.files[name]).unwrap();
    assert!(text("Cargo.toml").contains("features = [\"webrtc\"]"));
    assert!(text("Cargo.toml").contains("default-run = \"rtc-example\""));
    assert!(!text("Cargo.toml").contains("[patch.crates-io]"));
    assert!(text("src/main.rs").contains("rtc_example::run()"));
    assert!(text("src/lib.rs").contains("NativeRunner::new_with_webrtc(config, providers, webrtc)"));
    assert!(text("src/lib.rs").contains("run_webrtc_from_env(create_runner)"));
    assert!(plan.files.contains_key("src/bin/eval-bot.rs"));
    let client: serde_json::Value = serde_json::from_str(text("client/web/config.json")).unwrap();
    assert_eq!(client["transports"], serde_json::json!(["smallwebrtc"]));
    assert_eq!(client["start_endpoint"], "/api/start");
    assert!(text("README.md").contains("Normal `cargo run` enables SmallWebRTC"));
    assert!(text("README.md").contains("ICE is host-only"));
    assert!(!text("README.md").contains("SmallWebRTC server sessions, telephony"));
    let saved: ProjectConfig = serde_json::from_str(text("pipecat-project.json")).unwrap();
    assert_eq!(saved, plan.config);
    assert!(!plan.target.exists());
    assert_eq!(
        supported_options()["transport"],
        serde_json::json!(["websocket", "smallwebrtc"])
    );
}

#[test]
fn smallwebrtc_api_only_docs_and_websocket_default_do_not_substitute() {
    let temp = tempfile::tempdir().unwrap();
    let default = plan(&temp, "default-bot");
    assert!(!default.files.contains_key("src/lib.rs"));
    assert_eq!(
        default.files["src/main.rs"],
        include_bytes!("../templates/main.rs")
    );
    assert!(!std::str::from_utf8(&default.files["Cargo.toml"])
        .unwrap()
        .contains("features = [\"webrtc\"]"));
    let mut config = ProjectConfig::new("rtc-bot");
    config.transport = "smallwebrtc".into();
    let rtc = ProjectPlan::new(config, temp.path(), &source()).unwrap();
    let readme = std::str::from_utf8(&rtc.files["README.md"]).unwrap();
    assert!(readme.contains(r#"{"transport":"smallwebrtc"}"#));
    assert!(!readme.contains(r#"{"transport":"websocket"}"#));
}

#[test]
fn smallwebrtc_requires_local_native_and_vendored_sources_before_writing() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("Cargo.toml"), "[workspace]").unwrap();
    for name in ["pipecat-runner", "pipecat-webrtc"] {
        let path = source.join("crates").join(name);
        fs::create_dir_all(path.join("src")).unwrap();
        fs::write(path.join("Cargo.toml"), "[package]").unwrap();
        fs::write(path.join("src/lib.rs"), "").unwrap();
    }
    let mut config = ProjectConfig::new("rtc-bot");
    config.transport = "smallwebrtc".into();
    assert!(ProjectPlan::new(config.clone(), temp.path(), &source).is_err());
    let vendor = source.join("vendor/webrtc-0.14.0");
    fs::create_dir_all(vendor.join("src")).unwrap();
    fs::write(vendor.join("Cargo.toml"), "[package]").unwrap();
    assert!(ProjectPlan::new(config.clone(), temp.path(), &source).is_err());
    fs::write(vendor.join("src/lib.rs"), "").unwrap();
    assert!(ProjectPlan::new(config, temp.path(), &source).is_ok());
    assert!(!temp.path().join("rtc-bot").exists());
}

#[test]
fn generated_library_rejects_std_and_core_without_changing_websocket_names() {
    for name in ["std", "core"] {
        let mut config = ProjectConfig::new(name);
        config.validate().unwrap();
        config.transport = "smallwebrtc".into();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("standard library"));
    }
    let mut config = ProjectConfig::new("alloc");
    config.transport = "smallwebrtc".into();
    config.validate().unwrap();
}
