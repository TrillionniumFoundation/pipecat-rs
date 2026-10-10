use pipecat_flows::*;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

const MINIMAL: &str = "initial_node: a\nnodes:\n  a:\n    task_messages: []\n";
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pipecat-flow-yaml-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, text: &str) {
        let path = self.0.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    fn options(&self) -> FlowLoadOptions {
        FlowLoadOptions {
            includes: FlowIncludePolicy::Local {
                root: self.0.clone(),
            },
            ..Default::default()
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn source_baseline_examples_load_and_match_json_graphs() {
    // Fixtures are the six declarative examples at af9f9da9d7afddaa8811c0e0251b32566c980bdf.
    let fixtures = [
        include_str!("fixtures/source/hello_world.yaml"),
        include_str!("fixtures/source/food_ordering.yaml"),
        include_str!("fixtures/source/restaurant_reservation.yaml"),
        include_str!("fixtures/source/patient_intake.yaml"),
        include_str!("fixtures/source/insurance_quote.yaml"),
        include_str!("fixtures/source/podcast_interview.yaml"),
    ];
    for source in fixtures {
        let yaml = FlowConfig::from_yaml(source).unwrap();
        let json = FlowConfig::from_json(&serde_json::to_string(&yaml).unwrap()).unwrap();
        assert_eq!(yaml, json);
    }
}

#[test]
fn graph_features_and_typed_branch_keys_match_json() {
    let yaml = FlowConfig::from_yaml(
        r#"
initial_node: a
global_functions: [{name: status}]
nodes:
  a:
    task_messages: [{role: developer, content: "Hi {{ name }}"}]
    role_message: Native assistant
    context_strategy: reset
    respond_immediately: false
    pre_actions: [{type: custom, handler: prepare, parameter: [1, true, null]}]
    functions:
      - name: decide
        transition_to:
          field: result
          cases: {true: b, 3: a, 1.5: b, null: a}
          default: b
  b:
    task_messages: []
    functions: [{name: again, transition_only: true, description: Start again, transition_to: a}]
    post_actions: [{type: tts_say, text: Goodbye}, {type: end_conversation}]
"#,
    )
    .unwrap();
    let expected = json!({
        "initial_node":"a", "global_functions":[{"name":"status"}], "nodes": {
            "a": {"task_messages":[{"role":"developer","content":"Hi {{ name }}"}],
                "role_message":"Native assistant", "context_strategy":"reset", "respond_immediately":false,
                "pre_actions":[{"type":"custom","handler":"prepare","parameter":[1,true,null]}],
                "functions":[{"name":"decide","transition_to":{"field":"result","cases":{"true":"b","3":"a","1.5":"b","None":"a"},"default":"b"}}]},
            "b": {"task_messages":[], "functions":[{"name":"again","transition_only":true,"description":"Start again","transition_to":"a"}],
                "post_actions":[{"type":"tts_say","text":"Goodbye"},{"type":"end_conversation"}]}
        }
    });
    assert_eq!(yaml, FlowConfig::from_json(&expected.to_string()).unwrap());
}

#[test]
fn malformed_unknown_duplicate_and_python_fields_are_rejected() {
    for source in [
        "", "null", "[]", "nodes: [", "initial_node: a\nnodes: {}",
        "initial_node: a\nnodes: {a: {}}",
        "initial_node: a\nnodes: {a: {task_messages: [], surprise: 1}}",
        "initial_node: a\nnodes: {a: {task_messages: [{role: developer, content: hi, extra: 1}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, extra: 1}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, transition_to: missing}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, transition_to: {field: x, cases: {ok: a}, extra: true}}]}}",
        "initial_node: a\ninitial_node: a\nnodes: {a: {task_messages: []}}",
        "initial_node: a\nnodes: {a: {task_messages: []}, a: {task_messages: []}}",
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: custom, x: 1, x: 2}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: custom, values: {.nan: 1}}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: custom, values: .inf}]}}",
        "initial_node: a\nnodes: {a: {task_messages: []}}\n---\nignored: document",
        "initial_node: a\nnodes: {a: {task_messages: [], <<: {role_message: merged}}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, transition_to: {field: x, cases: {true: a, 'True': a}}}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, transition_to: {field: x, cases: {3: a, '3': a}}}]}}",
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, transition_to: {field: x, cases: {null: a, None: a}}}]}}",
        "initial_node: a\nnodes: {true: {task_messages: []}}",
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: x, parameters: {false: 2}}]}}",
    ] { assert!(FlowConfig::from_yaml(source).is_err(), "{source}"); }
    for text in [
        format!("{MINIMAL}handlers: handlers.py\n"),
        format!("{MINIMAL}handler_module: app.handlers\n"),
        "initial_node: a\nnodes: !!python/object:evil {a: {task_messages: []}}".into(),
        "initial_node: a\nnodes: !<tag:yaml.org,2002:python/object:evil> {a: {task_messages: []}}".into(),
        "initial_node: a\nnodes: !eval {a: {task_messages: []}}".into(),
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: app.handlers.x}]}}".into(),
        "initial_node: a\nnodes: {a: {task_messages: [], functions: [{name: x, handler: app.x}]}}".into(),
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: function, handler: app.x}]}}".into(),
        "initial_node: a\nnodes: {a: {task_messages: [], pre_actions: [{type: function, handler: {module: app, name: x}}]}}".into(),
    ] { assert!(matches!(FlowConfig::from_yaml(&text), Err(FlowError::Unsupported(_))), "{text}"); }
    assert!(FlowConfig::from_json(
        r#"{"initial_node":"a","nodes":{"a":{"task_messages":[]},"a":{"task_messages":[]}}}"#
    )
    .is_err());
    assert!(FlowConfig::from_json(MINIMAL).is_err());
    assert!(FlowConfig::from_json("{} {}").is_err());
}

#[test]
fn aliases_and_literal_tag_text_are_data() {
    let graph = FlowConfig::from_yaml(
        r#"
initial_node: a
nodes:
  a:
    task_messages: &messages
      - role: developer
        content: "!!python/object is literal; %TAG !<tag> too"
    role_message: |
      !!python/object is still literal
      %TAG !evil! tag:example
      !<tag>
  b:
    task_messages: *messages
"#,
    )
    .unwrap();
    assert_eq!(
        graph.nodes["a"].task_messages,
        graph.nodes["b"].task_messages
    );
    assert!(graph.nodes["a"]
        .role_message
        .as_ref()
        .unwrap()
        .contains("%TAG"));
    assert!(FlowConfig::from_yaml("a: &a [*a]").is_err());
}

#[test]
fn text_limits_apply_before_schema_or_duplicate_values_are_built() {
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_document_bytes: MINIMAL.len() - 1,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(MINIMAL, &options),
        Err(FlowError::Limit("flow document bytes"))
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_nodes: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        FlowConfig::from_json_with_options("[1,2,3]", &options),
        Err(FlowError::Limit("flow expanded nodes"))
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_depth: 65,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(FlowConfig::from_yaml_with_options(MINIMAL, &options)
        .unwrap_err()
        .to_string()
        .contains("hard ceilings"));
}

#[cfg(unix)]
#[test]
fn explicit_root_includes_use_fixed_base_and_default_loads_do_not_include() {
    let directory = Directory::new();
    directory.write(
        "flow.yaml",
        "initial_node: a\nnodes: !include sub/nodes.yaml\n",
    );
    directory.write(
        "sub/nodes.yaml",
        "a:\n  task_messages: !include messages.yaml\n",
    );
    directory.write(
        "messages.yaml",
        "- role: developer\n  content: Hello from a confined include\n",
    );
    let graph =
        FlowConfig::from_file_with_options(directory.0.join("flow.yaml"), &directory.options())
            .unwrap();
    assert_eq!(
        graph.nodes["a"].task_messages[0].content,
        "Hello from a confined include"
    );
    assert_eq!(
        graph,
        FlowConfig::from_yaml_with_options("!include flow.yaml", &directory.options()).unwrap()
    );
    assert!(matches!(
        FlowConfig::from_file(directory.0.join("flow.yaml")),
        Err(FlowError::Unsupported(_))
    ));
    assert!(matches!(
        FlowConfig::from_yaml("!include missing.yaml"),
        Err(FlowError::Unsupported(_))
    ));
    directory.write("plain.yml", MINIMAL);
    directory.write(
        "plain.json",
        r#"{"initial_node":"a","nodes":{"a":{"task_messages":[]}}}"#,
    );
    assert_eq!(
        FlowConfig::from_file(directory.0.join("plain.yml")).unwrap(),
        FlowConfig::from_file(directory.0.join("plain.json")).unwrap()
    );
    directory.write("plain.txt", MINIMAL);
    assert!(FlowConfig::from_file(directory.0.join("plain.txt")).is_err());
}

#[cfg(unix)]
#[test]
fn include_paths_cannot_escape_root_or_follow_symlinks_or_open_special_files() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let outside = Directory::new();
    outside.write("secret.yaml", MINIMAL);
    directory.write("safe.yaml", MINIMAL);
    symlink(
        outside.0.join("secret.yaml"),
        directory.0.join("final.yaml"),
    )
    .unwrap();
    symlink(&outside.0, directory.0.join("intermediate")).unwrap();
    symlink(
        directory.0.join("safe.yaml"),
        directory.0.join("internal.yaml"),
    )
    .unwrap();
    let options = directory.options();
    for path in [
        "../secret.yaml",
        "/etc/passwd",
        "file:///etc/passwd",
        "https://example.invalid/x",
        "C:\\Windows\\file",
        "final.yaml",
        "internal.yaml",
        "intermediate/secret.yaml",
        "missing.yaml",
        ".",
        "./safe.yaml",
        "sub/./safe.yaml",
        "sub/../safe.yaml",
        "sub//safe.yaml",
    ] {
        let text = format!("!include '{path}'");
        assert!(
            FlowConfig::from_yaml_with_options(&text, &options).is_err(),
            "{path}"
        );
    }
    assert!(FlowConfig::from_file_with_options(outside.0.join("secret.yaml"), &options).is_err());
    fs::create_dir(directory.0.join("folder")).unwrap();
    assert!(
        FlowConfig::from_yaml_with_options("!include folder", &options)
            .unwrap_err()
            .to_string()
            .contains("regular files")
    );
    rustix::fs::mknodat(
        rustix::fs::CWD,
        directory.0.join("fifo.yaml"),
        rustix::fs::FileType::Fifo,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        0,
    )
    .unwrap();
    assert!(
        FlowConfig::from_yaml_with_options("!include fifo.yaml", &options)
            .unwrap_err()
            .to_string()
            .contains("regular files")
    );
    assert!(FlowConfig::from_file(directory.0.join("fifo.yaml"))
        .unwrap_err()
        .to_string()
        .contains("regular files"));
    // An explicitly trusted root may itself be a symlink; child links never are.
    symlink(&directory.0, outside.0.join("trusted-root")).unwrap();
    let root_alias = FlowLoadOptions {
        includes: FlowIncludePolicy::Local {
            root: outside.0.join("trusted-root"),
        },
        ..Default::default()
    };
    assert!(FlowConfig::from_yaml_with_options("!include safe.yaml", &root_alias).is_ok());
    assert!(FlowConfig::from_file_with_options(
        outside.0.join("trusted-root/safe.yaml"),
        &root_alias
    )
    .is_ok());
}

#[cfg(unix)]
#[test]
fn include_cycles_depth_count_and_shared_construction_limits_are_bounded() {
    let directory = Directory::new();
    directory.write("a.yaml", "!include b.yaml");
    directory.write("b.yaml", "!include a.yaml");
    let options = directory.options();
    assert!(
        FlowConfig::from_file_with_options(directory.0.join("a.yaml"), &options)
            .unwrap_err()
            .to_string()
            .contains("cycle")
    );
    for index in 0..10 {
        directory.write(
            &format!("depth{index}.yaml"),
            &format!("!include depth{}.yaml", index + 1),
        );
    }
    directory.write("depth10.yaml", MINIMAL);
    assert_eq!(
        FlowConfig::from_yaml_with_options("!include depth0.yaml", &options),
        Err(FlowError::Limit("flow include depth"))
    );
    directory.write(
        "messages.yaml",
        "- role: developer\n  content: a long prompt reused by several nodes\n",
    );
    let graph = "initial_node: a\nnodes:\n  a:\n    task_messages: !include messages.yaml\n  b:\n    task_messages: !include messages.yaml\n";
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_include_files: 1,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(graph, &options),
        Err(FlowError::Limit("flow include files"))
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_total_bytes: graph.len() + 64,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(graph, &options),
        Err(FlowError::Limit("flow total source bytes"))
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_nodes: 27,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(graph, &options),
        Err(FlowError::Limit("flow expanded nodes"))
    );
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_expanded_scalar_bytes: 190,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options(graph, &options),
        Err(FlowError::Limit("flow expanded scalar bytes"))
    );
    directory.write("nested.yaml", "[[[[0]]]]");
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_depth: 5,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options("a: {b: {c: !include nested.yaml}}", &options),
        Err(FlowError::Limit("flow expanded depth"))
    );
    directory.write("oversize.yaml", &" ".repeat(300));
    let options = FlowLoadOptions {
        limits: FlowLoadLimits {
            max_document_bytes: 128,
            ..Default::default()
        },
        ..directory.options()
    };
    assert_eq!(
        FlowConfig::from_yaml_with_options("!include oversize.yaml", &options),
        Err(FlowError::Limit("flow document bytes"))
    );
    assert!(
        FlowConfig::from_yaml_with_options("!include [safe.yaml]", &directory.options()).is_err()
    );
}

#[tokio::test]
async fn yaml_and_json_drive_the_same_native_pipeline_transition() {
    use pipecat_core::{FrameProcessor, FunctionCall, Pipeline, ToolDefinition};
    use pipecat_services::Context;
    use pipecat_turns::LLMContextAggregatorPair;
    use std::sync::Arc;
    let source = r#"
initial_node: order
nodes:
  order:
    task_messages: [{role: developer, content: Ask for an item}]
    functions:
      - name: save_order
        transition_to: {field: saved, cases: {true: confirm}}
    respond_immediately: false
  confirm:
    task_messages: [{role: developer, content: "Confirm {{ item }}"}]
    respond_immediately: false
"#;
    let json = json!({"initial_node":"order","nodes":{
        "order":{"task_messages":[{"role":"developer","content":"Ask for an item"}],
        "functions":[{"name":"save_order","transition_to":{"field":"saved","cases":{"true":"confirm"}}}],"respond_immediately":false},
        "confirm":{"task_messages":[{"role":"developer","content":"Confirm {{ item }}"}],"respond_immediately":false}
    }});
    let mut contexts = Vec::new();
    for config in [
        FlowConfig::from_yaml(source).unwrap(),
        FlowConfig::from_json(&json.to_string()).unwrap(),
    ] {
        let mut handlers = HandlerRegistry::new();
        handlers
            .register_tool(
                "save_order",
                ToolHandler::new(
                    ToolDefinition {
                        name: "save_order".into(),
                        description: "Remember an item".into(),
                        parameters: json!({"type":"object"}),
                    },
                    |arguments, handle| async move {
                        handle.set_state("item", arguments["item"].clone())?;
                        Ok(ToolResult::configured(json!({"saved":true})))
                    },
                ),
            )
            .unwrap();
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        let sink = Arc::new(PipelineFlowSink::new());
        let manager = FlowManager::new(
            Flow::new(config, handlers).unwrap(),
            sink.clone(),
            pair.context(),
        );
        let processors: Vec<Arc<dyn FrameProcessor>> = vec![
            pair.user(),
            Arc::new(FlowProcessor::new(manager.clone())),
            pair.assistant(),
            Arc::new(FlowActionBoundary::new(manager.clone())),
        ];
        let worker = Pipeline::new(processors)
            .start(Default::default())
            .await
            .unwrap();
        sink.bind(worker.sender()).unwrap();
        manager.initialize().await.unwrap();
        let result = manager
            .execute_tools(vec![FunctionCall {
                function_name: "save_order".into(),
                tool_call_id: "same-call".into(),
                arguments: json!({"item":"pizza"}),
            }])
            .await
            .unwrap();
        assert_eq!(result.transitioned_to.as_deref(), Some("confirm"));
        contexts.push(serde_json::to_value(pair.context().snapshot().unwrap()).unwrap());
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{report:?}");
    }
    assert_eq!(contexts[0], contexts[1]);
    assert!(contexts[0].to_string().contains("Confirm pizza"));
}

#[test]
fn merge_keys_never_apply_unbounded_or_conflicting_updates() {
    for nodes in [
        "{a: &base {task_messages: []}, b: {<<: *base}}",
        "{a: &base {task_messages: []}, b: {<<: *base, task_messages: [{role: developer, content: override}]}}",
        "{a: &base {task_messages: []}, b: {<<: [*base, *base]}}",
    ] {
        assert!(matches!(FlowConfig::from_yaml(&format!("initial_node: a\nnodes: {nodes}")), Err(FlowError::Unsupported(_))));
    }
    assert!(FlowConfig::from_yaml(
        "initial_node: a\nnodes: {a: &base {task_messages: []}, b: {<<: *base, <<: *base}}"
    )
    .unwrap_err()
    .to_string()
    .contains("duplicate"));
}
