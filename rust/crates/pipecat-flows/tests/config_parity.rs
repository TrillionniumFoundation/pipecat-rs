use pipecat_flows::*;
use serde_json::json;

fn minimal() -> serde_json::Value {
    json!({"initial_node":"greet","nodes":{"greet":{"task_messages":[]},"done":{"task_messages":[]}}})
}
#[test]
fn defaults_and_json_roundtrip() {
    let config = FlowConfig::from_json(&minimal().to_string()).unwrap();
    assert!(config.nodes["greet"].respond_immediately);
    assert_eq!(config.nodes["greet"].context_strategy, None);
    assert_eq!(
        FlowConfig::from_json(&serde_json::to_string(&config).unwrap()).unwrap(),
        config
    );
}
#[test]
fn invalid_structures_are_rejected() {
    for text in [
        "[]",
        "null",
        "{}",
        r#"{"initial_node":"a","nodes":{}}"#,
        r#"{"initial_node":"a","nodes":{"a":{}}}"#,
    ] {
        assert!(FlowConfig::from_json(text).is_err(), "{text}");
    }
    let mut value = minimal();
    value["unknown"] = json!(1);
    assert!(FlowConfig::from_json(&value.to_string()).is_err());
    let mut value = minimal();
    value["initial_node"] = json!("missing");
    assert!(FlowConfig::from_json(&value.to_string()).is_err());
    for strategy in ["invalid", "reset_with_summary"] {
        let mut value = minimal();
        value["nodes"]["greet"]["context_strategy"] = json!(strategy);
        assert!(FlowConfig::from_json(&value.to_string()).is_err());
    }
}
#[test]
fn graph_function_and_action_validation() {
    let bad_functions = [
        json!([{"name":"a"},{"name":"a"}]),
        json!([{"name":"a","transition_to":"missing"}]),
        json!([{"name":"a","transition_to":{"field":"status","cases":{"ok":"done"},"default":"missing"}}]),
        json!([{"name":"a","transition_to":{"field":"status","cases":{}}}]),
        json!([{"name":"a","description":"not an edge"}]),
        json!([{"name":"a","transition_only":true,"transition_to":"done"}]),
        json!([{"name":"a","transition_only":true,"description":"go","transition_to":{"field":"status","cases":{"ok":"done"}}}]),
        json!([{"name":"a","unknown":1}]),
    ];
    for functions in bad_functions {
        let mut value = minimal();
        value["nodes"]["greet"]["functions"] = functions;
        assert!(
            FlowConfig::from_json(&value.to_string()).is_err(),
            "{value}"
        );
    }
    for action in [
        json!({"type":"function"}),
        json!({"type":"tts_say","handler":"x"}),
        json!({"type":"end_conversation","handler":"x"}),
    ] {
        let mut value = minimal();
        value["nodes"]["greet"]["pre_actions"] = json!([action]);
        assert!(FlowConfig::from_json(&value.to_string()).is_err());
    }
    let mut value = minimal();
    value["global_functions"] = json!([{"name":"global"}]);
    value["nodes"]["done"]["functions"] = json!([{"name":"global"}]);
    assert!(FlowConfig::from_json(&value.to_string()).is_err());
}
#[test]
fn branch_canonical_values_missing_field_and_default() {
    let branch = Transition::Branch(Branch {
        field: "status".into(),
        cases: [("True".into(), "yes".into()), ("3".into(), "three".into())].into(),
        default: Some("other".into()),
    });
    assert_eq!(
        branch.destination(&json!({"status":true})).unwrap(),
        Some("yes".into())
    );
    assert_eq!(
        branch.destination(&json!({"status":"TRUE"})).unwrap(),
        Some("yes".into())
    );
    assert_eq!(
        branch.destination(&json!({"status":3})).unwrap(),
        Some("three".into())
    );
    assert_eq!(
        branch.destination(&json!({"status":"unknown"})).unwrap(),
        Some("other".into())
    );
    assert!(branch.destination(&json!({})).is_err());
    assert!(branch.destination(&json!(true)).is_err());
}
#[test]
fn placeholder_source_cases() {
    let state = json!({"name":"Ada","order":{"size":"large"},"enabled":true,"missing":null})
        .as_object()
        .unwrap()
        .clone();
    assert_eq!(
        render_template(
            "Hi {{ name }}: {{order.size}}, {{enabled}} {{missing}}",
            &state
        )
        .unwrap(),
        "Hi Ada: large, True None"
    );
    assert_eq!(
        render_template(r"\{{ unknown }} {{ 2 + 3 }}", &state).unwrap(),
        "{{ unknown }} {{ 2 + 3 }}"
    );
    assert!(render_template("{{ order.missing }}", &state).is_err());
    assert!(render_template("{{ name.first }}", &state).is_err());
}
#[test]
fn reference_errors_are_collected_and_edges_need_no_handler() {
    let mut value = minimal();
    value["nodes"]["greet"]["functions"] = json!([{"name":"one"},{"name":"two"}]);
    value["nodes"]["greet"]["pre_actions"] = json!([{"type":"function","handler":"three"}]);
    let config = FlowConfig::from_json(&value.to_string()).unwrap();
    match Flow::new(config, HandlerRegistry::new()) {
        Err(FlowError::References(errors)) => assert_eq!(errors.len(), 3),
        _ => panic!("expected three missing references"),
    }
    let mut value = minimal();
    value["nodes"]["greet"]["functions"] =
        json!([{"name":"next","transition_only":true,"description":"Go","transition_to":"done"}]);
    Flow::new(
        FlowConfig::from_json(&value.to_string()).unwrap(),
        HandlerRegistry::new(),
    )
    .unwrap();
}
#[test]
fn include_requires_an_explicit_policy() {
    assert!(matches!(
        FlowConfig::from_yaml("!include ../secret"),
        Err(FlowError::Unsupported(_))
    ));
}
