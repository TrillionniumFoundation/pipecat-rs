//! Fixed Python request defaults, with the existing settings and HTTP regressions.
//! All network traffic is synthetic TCP loopback; this does not measure models.
#[path = "gemini_thought_boundaries.rs"]
mod http;
#[path = "http_simulated.rs"]
mod http_regressions;
#[path = "runtime_processor.rs"]
mod processor_regressions;
#[path = "runtime_settings.rs"]
mod settings_regressions;

use pipecat_core::{ChatMessage, ChatRole, LlmContextData, ToolSet};
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

fn defaults() -> [(&'static str, Value); 3] {
    [
        ("gemini-2.5-flash", json!({"thinkingBudget": 0})),
        ("gemini-3.6-flash", json!({"thinkingLevel": "minimal"})),
        ("gemini-3.8-flash", json!({"thinkingLevel": "low"})),
    ]
}

fn request(model: &str) -> CompletionRequest {
    CompletionRequest::new(
        model,
        Context::new(vec![Message::text(Role::User, "question")]),
    )
}

#[test]
fn exact_defaults_preserve_sampling_and_do_not_mutate_the_request() {
    for (model, thinking) in defaults() {
        for mode in 0..4 {
            let mut request = request(model);
            let mut generation = json!({"thinkingConfig": thinking});
            match mode {
                0 => {}
                1 => {
                    request.extra.insert("generationConfig".into(), json!({}));
                }
                2 => {
                    request.temperature = Some(0.25);
                    request.max_tokens = Some(32);
                    request.settings.top_p = Setting::Value(0.8);
                    request.settings.top_k = Setting::Value(9);
                    request.settings.seed = Setting::Value(42);
                    generation["temperature"] = json!(0.25);
                    generation["maxOutputTokens"] = json!(32);
                    generation["topP"] = json!(0.8);
                    generation["topK"] = json!(9);
                    generation["seed"] = json!(42);
                }
                3 => {
                    request.temperature = Some(0.25);
                    request.max_tokens = Some(32);
                    request.settings.temperature = Setting::Null;
                    request.settings.max_tokens = Setting::Null;
                }
                _ => unreachable!(),
            }
            let before = serde_json::to_value(&request).unwrap();
            let expected = json!({
                "contents": [{"role": "user", "parts": [{"text": "question"}]}],
                "generationConfig": generation,
            });
            assert_eq!(request_body(Provider::Gemini, &request).unwrap(), expected);
            assert_eq!(request_body(Provider::Gemini, &request).unwrap(), expected);
            assert_eq!(serde_json::to_value(&request).unwrap(), before);
        }
    }
}

#[test]
fn explicit_thinking_values_and_settings_extra_precedence_are_preserved() {
    for (model, _) in defaults() {
        for thinking in [
            json!({"thinkingBudget": 2048, "includeThoughts": true}),
            json!({"thinkingLevel": "high"}),
            json!(null),
            json!({}),
            json!(false),
            json!(0),
            json!("caller-value"),
            json!([]),
        ] {
            let generation = json!({
                "thinkingConfig": thinking,
                "temperature": 0.6,
                "responseMimeType": "application/json",
            });
            for from_settings in [false, true] {
                let mut request = request(model);
                request.extra.insert(
                    "generationConfig".into(),
                    if from_settings {
                        json!({"thinkingConfig": {"thinkingBudget": 99}, "topK": 4})
                    } else {
                        generation.clone()
                    },
                );
                if from_settings {
                    request
                        .settings
                        .base
                        .extra
                        .insert("generationConfig".into(), generation.clone());
                }
                let before = serde_json::to_value(&request).unwrap();
                assert_eq!(
                    request_body(Provider::Gemini, &request).unwrap()["generationConfig"],
                    generation,
                    "{model}, settings extra: {from_settings}"
                );
                assert_eq!(serde_json::to_value(&request).unwrap(), before);
            }
        }
    }
}

#[test]
fn nonobject_generation_and_existing_collision_errors_are_unchanged() {
    for (model, thinking) in defaults() {
        for generation in [json!(null), json!(false), json!(7), json!("raw"), json!([])] {
            for from_settings in [false, true] {
                let mut request = request(model);
                if from_settings {
                    request
                        .settings
                        .base
                        .extra
                        .insert("generationConfig".into(), generation.clone());
                } else {
                    request
                        .extra
                        .insert("generationConfig".into(), generation.clone());
                }
                assert_eq!(
                    request_body(Provider::Gemini, &request).unwrap()["generationConfig"],
                    generation
                );
            }
        }
        for from_settings in [false, true] {
            let mut request = request(model);
            request.settings.top_p = Setting::Value(0.8);
            let generation = json!({"thinkingConfig": null});
            if from_settings {
                request
                    .settings
                    .base
                    .extra
                    .insert("generationConfig".into(), generation);
            } else {
                request.extra.insert("generationConfig".into(), generation);
            }
            assert!(matches!(
                request_body(Provider::Gemini, &request),
                Err(ServiceError::InvalidRequest(message))
                    if message == "extra field generationConfig would override the streaming contract"
            ));
        }
        for count in [0, 2] {
            let mut request = request(model);
            request
                .extra
                .insert("generationConfig".into(), json!({"candidateCount": count}));
            assert!(matches!(
                request_body(Provider::Gemini, &request),
                Err(ServiceError::Unsupported(message)) if message == "multiple completion candidates"
            ));
        }
        let mut request = request(model);
        request
            .extra
            .insert("generationConfig".into(), json!({"candidateCount": 1}));
        assert_eq!(
            request_body(Provider::Gemini, &request).unwrap()["generationConfig"],
            json!({"candidateCount": 1, "thinkingConfig": thinking})
        );
    }
}

#[test]
fn only_the_resolved_exact_model_on_gemini_receives_a_default() {
    for model in [
        "unknown",
        "gemini-3.7-flash",
        "gemini-3.8-pro",
        "gemini-2.5-flash-lite",
        "models/gemini-2.5-flash",
        "models/gemini-3.6-flash",
        "models/gemini-3.8-flash",
        "gemini-3.6-flash-preview",
        "gemini-3.8-flash-latest",
        "gemini-3.8-flash-image",
        "Gemini-3.8-flash",
        "gemini-3.8-flash ",
    ] {
        let mut request = request("gemini-3.8-flash");
        request.settings.base.model = Setting::Value(model.into());
        assert!(
            request_body(Provider::Gemini, &request)
                .unwrap()
                .get("generationConfig")
                .is_none(),
            "{model}"
        );
    }
    for (model, thinking) in defaults() {
        let mut request = request("unknown");
        request.settings.base.model = Setting::Value(model.into());
        assert_eq!(
            request_body(Provider::Gemini, &request).unwrap()["generationConfig"],
            json!({"thinkingConfig": thinking})
        );
        for provider in [Provider::OpenAi, Provider::Anthropic] {
            let body = request_body(provider, &request).unwrap();
            assert_eq!(body["model"], model);
            assert!(body.get("generationConfig").is_none());
            assert!(body.get("thinking").is_none());
        }
    }
}

#[tokio::test]
async fn resolved_defaults_and_overrides_reach_http_without_mutating_snapshots() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = LlmClient::new(http::config(&listener)).unwrap();
    let mut initial = request("unknown");
    let sampling = json!({"temperature": 0.4, "topK": 9, "maxOutputTokens": 128});
    initial
        .settings
        .base
        .extra
        .insert("generationConfig".into(), sampling.clone());
    let processor = LlmProcessor::new(client.clone(), initial);
    let peer = tokio::spawn(async move {
        let mut bodies = vec![];
        for _ in 0..7 {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            bodies.push(http::read_request(&mut socket).await?);
            http::header(&mut socket).await?;
            http::chunk(
                &mut socket,
                &[
                    http::candidate(json!([{"text": "answer"}])),
                    http::stopped(),
                ],
            )
            .await?;
            http::eof(&mut socket).await?;
            socket.shutdown().await.map_err(|e| e.to_string())?;
        }
        Ok(bodies)
    });
    // Defer assertions until both the inference future and the peer are joined.
    // A timed-out inference drops its stream; join_peer aborts and joins a stuck peer.
    let observed = timeout(http::WAIT, async {
        let mut observations = vec![];
        let mut snapshots = vec![];
        for index in 0..7 {
            if index < 3 {
                let mut delta = LlmSettings::default();
                delta.base.model = Setting::Value(defaults()[index].0.into());
                processor
                    .update_settings(&delta)
                    .map_err(|e| e.to_string())?;
            }
            let mut options = InferenceOptions::default();
            if (3..6).contains(&index) {
                let thinking =
                    [json!({"thinkingBudget": 2048}), json!(null), json!({})][index - 3].clone();
                let mut generation = sampling.clone();
                generation["thinkingConfig"] = thinking;
                options
                    .settings
                    .base
                    .extra
                    .insert("generationConfig".into(), generation);
            }
            let snapshot = processor.request_snapshot().map_err(|e| e.to_string())?;
            let before = serde_json::to_value(&snapshot).map_err(|e| e.to_string())?;
            let mut outgoing = snapshot.clone();
            settings::merge_settings(&mut outgoing.settings, &options.settings);
            let body = client.request_body(&outgoing).map_err(|e| e.to_string())?;
            let answer = processor
                .run_inference(
                    LlmContextData {
                        messages: vec![ChatMessage::new(ChatRole::User, "question").into()],
                        tools: ToolSet::NotGiven,
                        tool_choice: None,
                    },
                    options,
                )
                .await
                .map_err(|e| e.to_string())?;
            let after =
                serde_json::to_value(processor.request_snapshot().map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            snapshots.push(snapshot);
            observations.push((body, before, after, answer));
        }
        Ok::<_, String>((observations, snapshots))
    })
    .await;
    let captured = http::join_peer(peer).await;
    let (observations, snapshots) = observed.expect("bounded inference sequence").unwrap();
    let captured = captured.unwrap();
    assert_eq!(captured.len(), 7);
    let thinking = [
        json!({"thinkingBudget": 0}),
        json!({"thinkingLevel": "minimal"}),
        json!({"thinkingLevel": "low"}),
        json!({"thinkingBudget": 2048}),
        json!(null),
        json!({}),
        json!({"thinkingLevel": "low"}),
    ];
    for (index, (body, before, after, answer)) in observations.into_iter().enumerate() {
        assert_eq!(answer.as_deref(), Some("answer"));
        assert_eq!(
            before, after,
            "inference must not persist derived defaults or overrides"
        );
        assert_eq!(
            captured[index], body,
            "request_body must match actual HTTP body"
        );
        let mut generation = sampling.clone();
        generation["thinkingConfig"] = thinking[index].clone();
        assert_eq!(
            captured[index],
            json!({
                "contents": [{"role": "user", "parts": [{"text": "question"}]}],
                "generationConfig": generation,
            })
        );
        assert_eq!(
            snapshots[index].settings.base.extra["generationConfig"],
            sampling
        );
        assert!(snapshots[index].extra.is_empty());
        assert_eq!(snapshots[index].model, "unknown");
    }
    for (index, (_, thinking)) in defaults().into_iter().enumerate() {
        assert_eq!(
            client.request_body(&snapshots[index]).unwrap()["generationConfig"]["thinkingConfig"],
            thinking,
            "later model updates must not rewrite earlier snapshots"
        );
    }
}
