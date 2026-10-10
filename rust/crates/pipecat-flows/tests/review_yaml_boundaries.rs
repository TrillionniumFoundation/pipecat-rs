use pipecat_flows::{FlowConfig, FlowError, FlowLoadLimits, FlowLoadOptions};

fn config_with_node_fields(fields: &str) -> String {
    format!("initial_node: a\nnodes:\n  a:\n    task_messages: []\n{fields}\n")
}

#[test]
fn review_global_tags_after_block_scalar_with_yaml_line_breaks() {
    for line_break in ["\n", "\r\n", "\r", "\u{85}", "\u{2028}", "\u{2029}"] {
        let source = config_with_node_fields(
            "    role_message: |\n      ordinary text\n    pre_actions: !!python/object:evil []",
        )
        .replace('\n', line_break);
        let result = FlowConfig::from_yaml(&source);
        assert!(
            matches!(result, Err(FlowError::Unsupported(_))),
            "line break {line_break:?}: {result:?}"
        );
    }
}

#[test]
fn review_unicode_yaml_breaks_cannot_hide_tags_in_block_or_comment_text() {
    for line_break in ["\u{85}", "\u{2028}", "\u{2029}"] {
        for fields in [
            format!(
                "    role_message: |\n      ordinary text{line_break}    pre_actions: !!python/object:evil []"
            ),
            format!(
                "    role_message: ordinary # comment{line_break}    pre_actions: !!python/object:evil []"
            ),
            format!(
                "    role_message: |\n      ordinary text{line_break}    pre_actions: !<tag:example.com,2026:evil> []"
            ),
        ] {
            let source = config_with_node_fields(&fields);
            let reference = serde_yaml::from_str::<serde_yaml::Value>(&source);
            assert!(reference.is_ok(), "bad fixture {source}\n{reference:?}");
            let result = FlowConfig::from_yaml(&source);
            assert!(
                matches!(result, Err(FlowError::Unsupported(_))),
                "line break {line_break:?}: {result:?}"
            );
        }
    }
}

#[test]
fn review_explicit_block_indentation_keeps_shallower_literal_lines() {
    for indicator in ["|2", "|2-", "|-2", ">2", ">2+", ">+2"] {
        let source = config_with_node_fields(&format!(
            "    role_message: {indicator}\n        deeply indented text\n      !!python/object:evil literal\n      %TAG !p! literal"
        ));
        let result = FlowConfig::from_yaml(&source);
        assert!(result.is_ok(), "indicator {indicator}: {result:?}");
    }
}

#[test]
fn review_rejects_unknown_non_specific_local_tag() {
    let source = "initial_node: ! a\nnodes: {a: {task_messages: []}}";
    let result = FlowConfig::from_yaml(source);
    assert!(
        matches!(result, Err(FlowError::Unsupported(_))),
        "{result:?}"
    );
}

#[test]
fn review_global_tags_across_flow_and_explicit_key_syntax() {
    let cases = [
        "initial_node: a\nnodes: {a:{task_messages: []}, b: !!python/object:evil {task_messages: []}}",
        "initial_node: a\nnodes: {a:{task_messages: []}, b:\n !!python/object:evil {task_messages: []}}",
        "initial_node: a\nnodes: {a: {task_messages:[], role_message: !!python/object:evil hi}}",
        "? initial_node\n: a\n? nodes\n: !!python/object:evil {a: {task_messages: []}}",
        "initial_node: a\nnodes:\n  ? a\n  : !!python/object:evil {task_messages: []}",
        "initial_node: a\nnodes:\n  a:\n    task_messages: []\n    pre_actions:\n    - type: custom\n      values: [\n        'literal',\n        !!python/object:evil {}\n        ]",
        "initial_node: a\nnodes:\n  a:\n    task_messages: []\n    pre_actions:\n    - type: custom\n      values: [a:{}, !!python/object:evil {}]",
    ];
    for source in cases {
        if serde_yaml::from_str::<serde_yaml::Value>(source).is_err() {
            continue;
        }
        let result = FlowConfig::from_yaml(source);
        assert!(
            matches!(result, Err(FlowError::Unsupported(_))),
            "{source}\n{result:?}"
        );
    }
}

#[test]
fn review_literal_tag_markers_in_yaml_scalar_forms() {
    let cases = [
        "    role_message: literal\n      !!python/object:evil is text\n      %TAG !p! still text",
        "    role_message:\n      literal\n      !!python/object:evil is text",
        "    role_message: \"\\\"escaped\n      !!python/object:evil is text\"",
        "    role_message: |\n\n      !!python/object:evil is text\n\n      %TAG !p! still text",
        "    role_message: |- # !!python/object:evil comment\n      literal text",
        "    role_message: literal#!!python/object:evil",
        "    role_message: 'unclosed on first line\n      !!python/object:evil is text'",
        "    role_message: 'escaped ''!!python/object:evil'' text'",
        "    role_message: 'literal' # \" !!python/object:evil\n    pre_actions: []",
    ];
    for fields in cases {
        let source = config_with_node_fields(fields);
        let reference = serde_yaml::from_str::<serde_yaml::Value>(&source);
        assert!(reference.is_ok(), "bad fixture {source}\n{reference:?}");
        let result = FlowConfig::from_yaml(&source);
        assert!(result.is_ok(), "{source}\n{result:?}");
    }
}

#[test]
fn review_alias_expansion_charges_every_scalar_before_schema_conversion() {
    let source = format!(
        "anchor: &text '{}'\ncopies: [{}]",
        "x".repeat(128),
        vec!["*text"; 64].join(",")
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_scalar_bytes: 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(&source, &options),
        Err(FlowError::Limit("flow expanded scalar bytes"))
    );
    let source = format!(
        "anchor: &row [{}]\ncopies: [{}]",
        vec!["0"; 128].join(","),
        vec!["*row"; 128].join(",")
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_nodes: 1024,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(&source, &options),
        Err(FlowError::Limit("flow expanded nodes"))
    );
}

#[test]
fn review_duplicate_values_are_not_expanded() {
    let source = format!("a: 1\na: '{}'", "x".repeat(1024));
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_scalar_bytes: 64,
            ..Default::default()
        },
        ..Default::default()
    };
    let error = FlowConfig::from_yaml_with_options(&source, &options).unwrap_err();
    assert!(matches!(error, FlowError::Config(_)), "{error:?}");
    assert!(error.to_string().contains("duplicate"), "{error:?}");
}

#[test]
fn review_normalized_branch_key_collisions_in_yaml_and_json() {
    for cases in [
        "{true: a, 'TRUE': a}",
        "{false: a, 'False': a}",
        "{null: a, 'None': a}",
        "{1.5: a, '1.5': a}",
        "{0x10: a, '16': a}",
        "{-1: a, '-1': a}",
        "{1: a, 0x1: a}",
        "{'true': a, 'TRUE': a}",
    ] {
        let source = config_with_node_fields(&format!(
            "    functions: [{{name: decide, transition_to: {{field: status, cases: {cases}}}}}]"
        ));
        let error = FlowConfig::from_yaml(&source).unwrap_err();
        assert!(
            error.to_string().contains("duplicate"),
            "{cases}: {error:?}"
        );
    }
    let source = r#"{"initial_node":"a","nodes":{"a":{"task_messages":[],"functions":[{"name":"decide","transition_to":{"field":"status","cases":{"True":"a","true":"a"}}}]}}}"#;
    let error = FlowConfig::from_json(source).unwrap_err();
    assert!(error.to_string().contains("duplicate"), "{error:?}");
}

#[cfg(unix)]
#[test]
fn review_fifo_include_root_rejects_without_blocking() {
    use pipecat_flows::FlowIncludePolicy;
    use rustix::fs::{mknodat, open, FileType, Mode, OFlags, CWD};
    use std::{fs, sync::mpsc, time::Duration};

    let root = std::env::temp_dir().join(format!(
        "pipecat-flow-review-fifo-root-{}",
        std::process::id()
    ));
    mknodat(CWD, &root, FileType::Fifo, Mode::RUSR | Mode::WUSR, 0).unwrap();
    let options = FlowLoadOptions {
        includes: FlowIncludePolicy::Local { root: root.clone() },
        ..Default::default()
    };
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = FlowConfig::from_yaml_with_options("{}", &options);
        let _ = sender.send(result);
    });
    let result = receiver.recv_timeout(Duration::from_millis(500));
    if result.is_err() {
        // A vulnerable blocking reader is released before this test fails.
        let _release = open(&root, OFlags::RDWR | OFlags::NONBLOCK, Mode::empty()).unwrap();
        let _ = receiver.recv_timeout(Duration::from_secs(1));
    }
    fs::remove_file(&root).unwrap();
    if result.is_ok() {
        worker.join().unwrap();
    }
    assert!(result.is_ok(), "opening a FIFO include root blocked");
    assert!(result.unwrap().is_err());
}

#[cfg(unix)]
#[test]
fn review_include_descriptors_resist_concurrent_child_symlink_swaps() {
    use pipecat_flows::FlowIncludePolicy;
    use std::{fs, os::unix::fs::symlink, sync::Arc};

    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    let fixture = Directory(
        std::env::temp_dir().join(format!("pipecat-flow-review-swap-{}", std::process::id())),
    );
    let root = fixture.0.join("root");
    let outside = fixture.0.join("outside");
    fs::create_dir_all(root.join("folder")).unwrap();
    fs::create_dir_all(&outside).unwrap();
    let allowed = "initial_node: authorized\nnodes: {authorized: {task_messages: []}}";
    let sentinel = "initial_node: outside\nnodes: {outside: {task_messages: []}}";
    fs::write(root.join("file.yaml"), allowed).unwrap();
    fs::write(root.join("folder/flow.yaml"), allowed).unwrap();
    fs::write(outside.join("flow.yaml"), sentinel).unwrap();
    let options = FlowLoadOptions {
        includes: FlowIncludePolicy::Local { root: root.clone() },
        ..Default::default()
    };
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        for _ in 0..500 {
            symlink(outside.join("flow.yaml"), root.join("link.next")).unwrap();
            fs::rename(root.join("link.next"), root.join("file.yaml")).unwrap();
            fs::write(root.join("file.next"), allowed).unwrap();
            fs::rename(root.join("file.next"), root.join("file.yaml")).unwrap();
            fs::rename(root.join("folder"), root.join("parked")).unwrap();
            symlink(&outside, root.join("folder")).unwrap();
            fs::remove_file(root.join("folder")).unwrap();
            fs::rename(root.join("parked"), root.join("folder")).unwrap();
        }
    });
    barrier.wait();
    let mut unauthorized = Vec::new();
    for _ in 0..500 {
        for path in ["file.yaml", "folder/flow.yaml"] {
            if let Ok(graph) =
                FlowConfig::from_yaml_with_options(&format!("!include {path}"), &options)
            {
                if graph.initial_node != "authorized" {
                    unauthorized.push(graph.initial_node);
                }
            }
        }
    }
    worker.join().unwrap();
    assert!(
        unauthorized.is_empty(),
        "read a symlink target outside the root"
    );
    for path in ["file.yaml", "folder/flow.yaml"] {
        let graph =
            FlowConfig::from_yaml_with_options(&format!("!include {path}"), &options).unwrap();
        assert_eq!(graph.initial_node, "authorized");
    }
}

#[test]
fn review_preserves_unicode_scalar_content_while_scanning_line_boundaries() {
    for text in [
        "quoted \u{85}      content",
        "quoted \u{2028}      content",
        "quoted \u{2029}      content",
    ] {
        let source = config_with_node_fields(&format!("    role_message: '{text}'"));
        let reference = serde_yaml::from_str::<serde_yaml::Value>(&source).unwrap();
        let graph = FlowConfig::from_yaml(&source).unwrap();
        assert_eq!(
            graph.nodes["a"].role_message.as_deref(),
            reference["nodes"]["a"]["role_message"].as_str()
        );
    }
    for (escape, character) in [("\\N", '\u{85}'), ("\\L", '\u{2028}'), ("\\P", '\u{2029}')] {
        let source =
            config_with_node_fields(&format!("    role_message: \"quoted {escape} content\""));
        let graph = FlowConfig::from_yaml(&source).unwrap();
        assert_eq!(
            graph.nodes["a"].role_message.as_deref(),
            Some(format!("quoted {character} content").as_str())
        );
    }
}

#[test]
fn review_parser_disagreement_on_indentation_fails_closed() {
    let mut cases = vec![
        "    role_message: 'quoted\nnot indented'".to_string(),
        "    pre_actions:\n    - type: custom\n      values: [\n        'literal'\n      ]"
            .to_string(),
    ];
    for line_break in ['\u{85}', '\u{2028}', '\u{2029}'] {
        cases.push(format!("    role_message: 'quoted {line_break} content'"));
    }
    for fields in cases {
        let source = config_with_node_fields(&fields);
        // Libyaml accepts these forms. The token scanner requires indentation
        // after every line boundary; disagreement must not bypass tag checks.
        assert!(serde_yaml::from_str::<serde_yaml::Value>(&source).is_ok());
        let error = FlowConfig::from_yaml(&source).unwrap_err();
        assert!(matches!(error, FlowError::Config(_)), "{error:?}");
        assert!(error.to_string().contains("indentation"), "{error:?}");
    }
}
