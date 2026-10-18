//! Opt-in plain image route. Full-route tests end at ordinary assistant history,
//! actual native LLM HTTP, RTVI response observations, and joined owners.
#[path = "support/image_fixture.rs"]
mod fixture;
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
#[path = "support/image_app.rs"]
mod image_app;
use fixture::*;
use pipecat_evals::{ScenarioFile, ScriptScenario, ScriptTurn, SessionOptions};
#[cfg(feature = "user-images")]
use serde_json::json;

#[test]
fn ordinary_loader_and_public_struct_literals_remain_text_only() {
    let bundle = Bundle::new();
    bundle.put("image.png", png_rgb(2, 2, [23, 81, 143]));
    assert!(ScenarioFile::load(bundle.source("image.png", ANSWER)).is_err());
    let turn = ScriptTurn {
        user: Some("ordinary text".into()),
        dtmf: None,
        expect: vec![],
        send_after: None,
    };
    let scenario = ScriptScenario {
        name: "literal compatibility".into(),
        judge: None,
        turns: vec![turn],
        context: vec![],
        stop_on_failure: true,
        trigger_disconnect: false,
    };
    let base = SessionOptions::default();
    let options = SessionOptions {
        transport: base.transport,
        handshake_timeout: base.handshake_timeout,
        default_timeout: base.default_timeout,
        send_after_timeout: base.send_after_timeout,
        bot_quiet_timeout: base.bot_quiet_timeout,
        max_duration: base.max_duration,
        event_limits: base.event_limits,
        max_wire_messages: base.max_wire_messages,
        max_wire_bytes: base.max_wire_bytes,
        stop_bot: base.stop_bot,
    };
    assert!(pipecat_evals::EvalSession::new(scenario, "ws://127.0.0.1:1", options).is_ok());
}

#[cfg(feature = "user-images")]
mod loader {
    use super::*;
    use pipecat_evals::{EncodedUserImage, EvalSession, UserImageScenarioFile};

    #[test]
    fn explicit_loader_accepts_synthetic_png_jpeg_and_rejects_unsafe_or_conflicting_references() {
        let bundle = Bundle::new();
        bundle.put("good.png", png_rgb(3, 2, [24, 80, 140]));
        bundle.put("good.jpeg", jpeg_rgb(3, 2, [24, 80, 140]));
        for path in ["good.png", "good.jpeg"] {
            let file = UserImageScenarioFile::load(bundle.source(path, ANSWER)).unwrap();
            assert_eq!(
                file.into_sessions("ws://127.0.0.1:1", SessionOptions::default())
                    .unwrap()
                    .len(),
                1
            );
        }
        for (name, bytes) in [
            ("empty.png", vec![]),
            ("wrong.jpg", png_rgb(1, 1, [0, 0, 0])),
            ("unknown.bin", png_rgb(1, 1, [0, 0, 0])),
            ("unsupported.gif", b"GIF89a".to_vec()),
        ] {
            bundle.put(name, bytes);
            assert!(
                UserImageScenarioFile::load(bundle.source(name, ANSWER)).is_err(),
                "accepted {name}"
            );
        }
        for reference in [
            "https://invalid.example/personal.png",
            "data:image/png;base64,AAAA",
            "../escape.png",
            "/dev/zero",
            "missing.png",
        ] {
            assert!(
                UserImageScenarioFile::load(bundle.source(reference, ANSWER)).is_err(),
                "accepted {reference}"
            );
        }
        for turn in [
            json!({"image":1,"user":"x"}),
            json!({"image":"good.png","audio":"x.wav","user":"x"}),
            json!({"image":"good.png","dtmf":"1"}),
            json!({"image":"good.png","send_after":{"delay_ms":1}}),
        ] {
            let file = bundle.json(
                "bad.json",
                &json!({"name":"unsupported input", "turns":[turn]}),
            );
            assert!(UserImageScenarioFile::load(file).is_err());
        }
    }
    #[test]
    fn whole_block_includes_use_the_outer_bundle_image_root() {
        let bundle = Bundle::new();
        std::fs::create_dir_all(bundle.0.join("sub/deeper")).unwrap();
        bundle.put("image.png", png_rgb(2, 2, [40, 80, 120]));
        bundle.put("sub/image.png", b"invalid decoy must not be read");
        bundle.put(
            "sub/deeper/turns.yaml",
            "- image: image.png\n  user: inspect\n",
        );
        bundle.put("sub/turns.yaml", "!include deeper/turns.yaml\n");
        let path = bundle.put("group.yaml", "name: grouped\nuser: {modality: text}\nturns: !include sub/turns.yaml\nscenarios:\n - name: first\n - name: second\n");
        let sessions = UserImageScenarioFile::load(path)
            .unwrap()
            .into_sessions("ws://127.0.0.1:1", SessionOptions::default())
            .unwrap();
        assert_eq!(sessions.len(), 2);
        let replaced = bundle.put("replace.yaml", "name: grouped\nuser: {modality: text}\nturns: [{image: image.png, user: inspect}]\nscenarios:\n - name: child\n   user: {modality: audio}\n");
        assert!(UserImageScenarioFile::load(replaced).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn local_image_references_cannot_escape_through_symlink_or_directory() {
        let bundle = Bundle::new();
        let outside = Bundle::new();
        let image = outside.put("outside.png", png_rgb(1, 1, [1, 2, 3]));
        std::os::unix::fs::symlink(image, bundle.0.join("escape.png")).unwrap();
        std::fs::create_dir(bundle.0.join("directory.png")).unwrap();
        for name in ["escape.png", "directory.png"] {
            assert!(UserImageScenarioFile::load(bundle.source(name, ANSWER)).is_err());
        }
    }
    #[test]
    fn typed_sidecar_rejects_invalid_indexes_duplicates_mime_and_budgets_before_connect() {
        for (bytes, mime) in [
            (vec![], "image/png"),
            (png_rgb(1, 1, [1, 2, 3]), "image/jpeg"),
            (vec![1; 65_537], "image/png"),
            (png_rgb(1, 1, [1, 2, 3]), "image/gif"),
        ] {
            assert!(EncodedUserImage::new(bytes, mime).is_err());
        }
        let scenario = || {
            ScenarioFile::from_value(json!({"name":"typed", "turns":[{"user":"inspect"}]}))
                .unwrap()
                .scenarios
                .remove(0)
        };
        let image = || EncodedUserImage::new(png_rgb(1, 1, [2, 4, 6]), "image/png").unwrap();
        assert!(
            EvalSession::new(scenario(), "ws://127.0.0.1:1", SessionOptions::default())
                .unwrap()
                .with_user_images([(1, image())])
                .is_err()
        );
        assert!(
            EvalSession::new(scenario(), "ws://127.0.0.1:1", SessionOptions::default())
                .unwrap()
                .with_user_images([(0, image()), (0, image())])
                .is_err()
        );
        let mut small = SessionOptions::default();
        small.transport.max_message_bytes = 100;
        assert!(EvalSession::new(scenario(), "ws://127.0.0.1:1", small)
            .unwrap()
            .with_user_images([(0, image())])
            .is_err());
    }
    #[test]
    fn oversized_encoded_bundle_is_rejected_before_runtime() {
        let bundle = Bundle::new();
        let mut bytes = png_rgb(1, 1, [1, 2, 3]);
        bytes.resize(65_537, 0);
        bundle.put("large.png", bytes);
        assert!(UserImageScenarioFile::load(bundle.source("large.png", ANSWER)).is_err());
    }
}

#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
mod chain {
    use super::*;
    use image_app::{ImageBot, InputPolicy};
    use pipecat_core::{ChatRole, MessageContent, MessagePart};
    use pipecat_evals::{serializer, Cancellation, ImageInputLimits, UserImageScenarioFile};
    use std::sync::atomic::Ordering;
    use tokio::time::timeout;

    async fn run_fixture(bytes: Vec<u8>, extension: &str, expected_rgb: Vec<u8>, size: (u32, u32)) {
        let bundle = Bundle::new();
        bundle.put(&format!("synthetic.{extension}"), bytes);
        let path = bundle.source(&format!("synthetic.{extension}"), ANSWER);
        let http = Http::start().await;
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let session = UserImageScenarioFile::load(path)
            .unwrap()
            .into_sessions(&host.url, options())
            .unwrap()
            .remove(0);
        let result = session.run(Cancellation::new()).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        let requests = trace.requests.lock().unwrap();
        let images = trace.images.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].1, TEXT);
        assert_eq!(images.len(), 1);
        let image = &images[0];
        assert_ne!(image.id, requests[0].0);
        assert_eq!(image.request_id, requests[0].0);
        assert_eq!(trace.assistant_image_calls.load(Ordering::SeqCst), 1);
        assert!(image.source.is_none() && image.destination.is_none());
        assert_eq!(image.rgb, expected_rgb);
        assert_eq!(image.size, size);
        assert_eq!(image.format.as_deref(), Some("RGB"));
        assert_eq!(image.user_id, "synthetic-eval-user");
        assert_eq!(image.text.as_deref(), Some(TEXT));
        assert_eq!(image.append_to_context, Some(true));
        let request = image.request.as_ref().unwrap();
        assert_eq!(request.user_id, image.user_id);
        assert_eq!(request.text, image.text);
        assert_eq!(request.append_to_context, image.append_to_context);
        assert_eq!(request.video_source.as_deref(), Some("camera"));
        assert!(
            request.function_name.is_none()
                && request.tool_call_id.is_none()
                && request.result_callback.is_none()
        );
        let context = trace.contexts.lock().unwrap()[0].snapshot_rich().unwrap();
        assert_eq!(
            context
                .messages
                .iter()
                .filter(|m| m.role == ChatRole::User)
                .count(),
            1
        );
        assert_eq!(
            context
                .messages
                .iter()
                .filter(|m| m.role == ChatRole::Assistant)
                .count(),
            1
        );
        let MessageContent::Parts(parts) = &context.messages[0].content else {
            panic!("standard consumer did not own image parts");
        };
        assert!(matches!(&parts[0], MessagePart::Text { text } if text == TEXT));
        let bodies = http.requests.lock().unwrap();
        assert_eq!(bodies.len(), 1, "more than one native inference");
        assert_http_image(&bodies[0], TEXT, size, &expected_rgb, 1);
        assert!(!result
            .events_seen
            .iter()
            .any(|event| event["type"] == "function_call"));
        assert!(result
            .events_seen
            .iter()
            .any(|event| event["type"] == "llm_response"));
    }
    #[tokio::test]
    async fn source_yaml_png_to_real_rgb_consumer_http_and_observed_response() {
        run_fixture(
            png_rgb(16, 12, [24, 80, 140]),
            "png",
            rgb(16, 12, [24, 80, 140]),
            (16, 12),
        )
        .await;
    }
    #[tokio::test]
    async fn source_yaml_jpeg_to_rgb_and_standard_jpeg_http_has_corresponding_pixels() {
        let input = jpeg_rgb(16, 12, [24, 80, 140]);
        let (size, pixels) = jpeg_pixels(&input);
        run_fixture(input, "jpeg", pixels, size).await;
    }
    #[tokio::test]
    async fn palette_grayscale_and_alpha_png_use_the_actual_rgb_conversion_subset() {
        for (color, pixels, palette, expected) in [
            (
                png::ColorType::Indexed,
                vec![0; 16],
                Some(vec![24, 80, 140]),
                rgb(4, 4, [24, 80, 140]),
            ),
            (
                png::ColorType::Grayscale,
                vec![73; 16],
                None,
                rgb(4, 4, [73; 3]),
            ),
            // Native source-compatible RGB conversion strips, not composites, alpha.
            (
                png::ColorType::Rgba,
                [24, 80, 140, 0].repeat(16),
                None,
                rgb(4, 4, [24, 80, 140]),
            ),
        ] {
            run_fixture(
                png_image(4, 4, color, &pixels, palette.as_deref()),
                "png",
                expected,
                (4, 4),
            )
            .await;
        }
    }
    #[tokio::test]
    async fn owned_bundle_bytes_survive_path_replacement_and_omitted_or_null_images_persist() {
        let bundle = Bundle::new();
        let original = png_rgb(8, 8, [24, 80, 140]);
        bundle.put("a.png", &original);
        let path = bundle.json("scenario.json", &json!({"name":"persistent latest", "turns":[
            {"image":"a.png","user":"first","expect":[{"event":"response","text_contains":ANSWER}]},
            {"user":"second","expect":[{"event":"response","text_contains":ANSWER}]},
            {"image":null,"user":"third","expect":[{"event":"response","text_contains":ANSWER}]}
        ]}));
        let file = UserImageScenarioFile::load(path).unwrap();
        bundle.put("a.png", png_rgb(8, 8, [150, 30, 70]));
        let http = Http::start().await;
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let result = file
            .into_sessions(&host.url, options())
            .unwrap()
            .remove(0)
            .run(Cancellation::new())
            .await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        let images = trace.images.lock().unwrap();
        assert_eq!(images.len(), 3);
        assert!(images
            .iter()
            .all(|image| image.rgb == rgb(8, 8, [24, 80, 140])));
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for (index, text) in ["first", "second", "third"].into_iter().enumerate() {
            assert_http_image(
                &requests[index],
                text,
                (8, 8),
                &rgb(8, 8, [24, 80, 140]),
                index + 1,
            );
        }
    }
    #[tokio::test]
    async fn image_only_registration_is_passive_and_does_not_emit_synthetic_success() {
        let bundle = Bundle::new();
        bundle.put("a.png", png_rgb(4, 4, [24, 80, 140]));
        let path = bundle.json("scenario.json", &json!({"name":"passive image registration", "turns":[{"image":"a.png", "expect":[{"event":"response","absent":true,"within_ms":80}]}]}));
        let http = Http::start().await;
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let result = UserImageScenarioFile::load(path)
            .unwrap()
            .into_sessions(&host.url, options())
            .unwrap()
            .remove(0)
            .run(Cancellation::new())
            .await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        assert!(trace.requests.lock().unwrap().is_empty());
        assert!(trace.images.lock().unwrap().is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
        assert!(trace.contexts.lock().unwrap()[0]
            .snapshot_rich()
            .unwrap()
            .messages
            .is_empty());
    }
    #[tokio::test]
    async fn ordinary_text_application_gets_no_image_or_extra_inference() {
        let http = Http::start().await;
        let mut bot = ImageBot::new(http.url.clone()).unwrap();
        bot.policy = InputPolicy::OrdinaryText;
        let host = Host::start(bot).await;
        let mut socket = connect(&host.url).await.unwrap();
        let outcome: Result<_, Error> = async {
            register(&socket, &png_rgb(2, 2, [24, 80, 140]), "image/png").await?;
            text(&socket, "ordinary text").await?;
            response(&mut socket).await
        }
        .await;
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert_eq!(outcome.unwrap(), ANSWER);
        assert!(report.is_ok());
        assert_joined(&trace, 1, &http);
        assert!(trace.images.lock().unwrap().is_empty());
        assert!(trace.requests.lock().unwrap().is_empty());
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["messages"][0]["content"], "ordinary text");
    }
    #[tokio::test]
    async fn passive_requested_image_is_real_rgb_but_never_appends_or_runs() {
        let http = Http::start().await;
        let mut bot = ImageBot::new(http.url.clone()).unwrap();
        bot.policy = InputPolicy::PassiveImage;
        let host = Host::start(bot).await;
        let socket = connect(&host.url).await.unwrap();
        let outcome: Result<_, Error> = async {
            register(&socket, &png_rgb(2, 2, [24, 80, 140]), "image/png").await?;
            text(&socket, "passive").await?;
            timeout(WAIT, host.trace.assistant_image_arrived.notified()).await?;
            Ok(())
        }
        .await;
        // A real next wire command traverses the host; completion never relies
        // on the registration's socket-write admission.
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        outcome.unwrap();
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        let images = trace.images.lock().unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].rgb, rgb(2, 2, [24, 80, 140]));
        assert_eq!(images[0].append_to_context, Some(false));
        assert!(trace.contexts.lock().unwrap()[0]
            .snapshot_rich()
            .unwrap()
            .messages
            .is_empty());
        assert!(http.requests.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn compressed_pixels_above_wire_json_ceiling_pass_only_native_media_budget() {
        // 196,608 RGB bytes exceed 128 KiB even before decimal JSON expansion.
        run_fixture(
            png_rgb(256, 256, [24, 80, 140]),
            "png",
            rgb(256, 256, [24, 80, 140]),
            (256, 256),
        )
        .await;
    }
    #[tokio::test]
    async fn later_registration_replaces_image_for_the_next_real_request() {
        let http = Http::start().await;
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let mut socket = connect(&host.url).await.unwrap();
        let outcome: Result<(), Error> = async {
            register(&socket, &png_rgb(4, 4, [24, 80, 140]), "image/png").await?;
            text(&socket, "first").await?;
            assert_eq!(response(&mut socket).await?, ANSWER);
            register(&socket, &png_rgb(4, 4, [150, 30, 70]), "image/png").await?;
            text(&socket, "second").await?;
            assert_eq!(response(&mut socket).await?, ANSWER);
            Ok(())
        }
        .await;
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        outcome.unwrap();
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        let images = trace.images.lock().unwrap();
        assert_eq!(images.len(), 2);
        assert_eq!(images[0].rgb, rgb(4, 4, [24, 80, 140]));
        assert_eq!(images[1].rgb, rgb(4, 4, [150, 30, 70]));
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_http_image(&requests[1], "second", (4, 4), &rgb(4, 4, [150, 30, 70]), 2);
    }
    #[tokio::test]
    async fn real_immediate_input_interruption_fences_an_already_emitted_system_image() {
        let http = Http::start().await;
        let bot = ImageBot::new(http.url.clone()).unwrap();
        bot.trace.hold_next_image.store(true, Ordering::SeqCst);
        let host = Host::start(bot).await;
        let mut socket = connect(&host.url).await.unwrap();
        let outcome: Result<_, Error> = async {
            register(&socket, &png_rgb(4, 4, [24, 80, 140]), "image/png").await?;
            text(&socket, "stale original").await?;
            timeout(WAIT, host.trace.image_arrived.notified())
                .await
                .map_err(|error| format!("waiting for original held RGB: {error}"))?;
            let original_fence = host
                .trace
                .held_image_fence
                .lock()
                .unwrap()
                .take()
                .ok_or("original held RGB has no causal fence")?;
            assert!(original_fence.is_current());
            assert!(http.trace.requests.lock().unwrap().is_empty());
            register(&socket, &png_rgb(4, 4, [150, 30, 70]), "image/png").await?;
            text(&socket, "current replacement").await?;
            // The held System call blocks this router's later interruption and
            // normal append. Observe retirement of the genuine image request's
            // upstream causal scope instead of waiting for that blocked append.
            timeout(WAIT, async {
                while original_fence.is_current() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|error| {
                format!(
                    "waiting for original request fence retirement: {error}; requests={}, images={}, interruptions={}, HTTP={}",
                    host.trace.requests.lock().unwrap().len(),
                    host.trace.images.lock().unwrap().len(),
                    host.trace.interruptions.load(Ordering::SeqCst),
                    http.trace.requests.lock().unwrap().len(),
                )
            })?;
            host.trace.image_gate.add_permits(1);
            response(&mut socket).await.map_err(|error| {
                format!(
                    "waiting for replacement native response: {error}; requests={}, images={}, interruptions={}, HTTP={}",
                    host.trace.requests.lock().unwrap().len(),
                    host.trace.images.lock().unwrap().len(),
                    host.trace.interruptions.load(Ordering::SeqCst),
                    http.trace.requests.lock().unwrap().len(),
                )
                .into()
            })
        }
        .await;
        host.trace.image_gate.add_permits(1);
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert_eq!(outcome.unwrap(), ANSWER);
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        assert!(trace.interruptions.load(Ordering::SeqCst) >= 2);
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_http_image(
            &requests[0],
            "current replacement",
            (4, 4),
            &rgb(4, 4, [150, 30, 70]),
            1,
        );
        let context = trace.contexts.lock().unwrap()[0].snapshot_rich().unwrap();
        assert_eq!(
            context
                .messages
                .iter()
                .filter(|message| message.role == ChatRole::User)
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn interruption_suppresses_queued_derived_context_before_actual_native_http() {
        let http = Http::start().await;
        let bot = ImageBot::new(http.url.clone()).unwrap();
        bot.trace.hold_next_context.store(true, Ordering::SeqCst);
        let host = Host::start(bot).await;
        let mut socket = connect(&host.url).await.unwrap();
        let outcome: Result<_, Error> = async {
            register(&socket, &png_rgb(4, 4, [24, 80, 140]), "image/png").await?;
            text(&socket, "committed before interruption").await?;
            timeout(WAIT, host.trace.context_arrived.notified()).await?;
            assert!(http.trace.requests.lock().unwrap().is_empty());
            assert_eq!(
                host.trace.contexts.lock().unwrap()[0]
                    .snapshot_rich()
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            register(&socket, &png_rgb(4, 4, [150, 30, 70]), "image/png").await?;
            text(&socket, "new generation inference").await?;
            timeout(WAIT, async {
                loop {
                    if host.trace.requests.lock().unwrap().len() >= 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            host.trace.context_gate.add_permits(1);
            response(&mut socket).await
        }
        .await;
        host.trace.context_gate.add_permits(1);
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        assert_eq!(outcome.unwrap(), ANSWER);
        assert!(report.is_ok(), "{report:?}");
        assert_joined(&trace, 1, &http);
        let requests = http.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            1,
            "stale queued context reached real native HTTP"
        );
        assert_http_image(
            &requests[0],
            "new generation inference",
            (4, 4),
            &rgb(4, 4, [150, 30, 70]),
            2,
        );
        assert_eq!(
            requests[0]["messages"][0]["content"][0]["text"],
            "committed before interruption"
        );
    }

    #[tokio::test]
    async fn fresh_websocket_session_has_no_prior_registered_image() {
        let http = Http::start().await;
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let mut first = connect(&host.url).await.unwrap();
        register(&first, &png_rgb(2, 2, [24, 80, 140]), "image/png")
            .await
            .unwrap();
        text(&first, "first session").await.unwrap();
        let first_response = response(&mut first).await;
        close(first).await;
        let mut second = connect(&host.url).await.unwrap();
        text(&second, "must not inherit").await.unwrap();
        let second_response = response(&mut second).await;
        close(second).await;
        let (_report, trace) = host.finish().await;
        let http = http.finish().await;
        assert_eq!(first_response.unwrap(), ANSWER);
        assert!(second_response.is_err());
        assert_joined(&trace, 2, &http);
        assert_eq!(trace.images.lock().unwrap().len(), 1);
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        assert!(trace.contexts.lock().unwrap()[1]
            .snapshot_rich()
            .unwrap()
            .messages
            .is_empty());
    }
    #[tokio::test]
    async fn malformed_wire_images_and_default_host_never_reach_consumer_or_http() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let valid = png_rgb(4, 4, [24, 80, 140]);
        let mut trailing = valid.clone();
        trailing.push(0);
        let mut truncated = valid.clone();
        truncated.truncate(truncated.len() - 8);
        let cases = vec![
            (true, json!({"image":"%%%", "format":"image/png"})),
            (true, json!({"image":"", "format":"image/png"})),
            (
                true,
                json!({"image":STANDARD.encode(&valid), "format":"image/jpeg"}),
            ),
            (
                true,
                json!({"image":STANDARD.encode(truncated), "format":"image/png"}),
            ),
            (
                true,
                json!({"image":STANDARD.encode(trailing), "format":"image/png"}),
            ),
            (
                false,
                json!({"image":STANDARD.encode(&valid), "format":"image/png"}),
            ),
        ];
        for (enabled, payload) in cases {
            let http = Http::start().await;
            let host = Host::start_with(
                ImageBot::new(http.url.clone()).unwrap(),
                config(),
                enabled.then(ImageInputLimits::default),
            )
            .await;
            let mut socket = connect(&host.url).await.unwrap();
            let outcome: Result<String, Error> = async {
                send(
                    &socket,
                    serializer::control(3, serializer::EVAL_IMAGE, payload),
                )
                .await?;
                text(&socket, "must fail").await?;
                response(&mut socket).await
            }
            .await;
            close(socket).await;
            let (_report, trace) = host.finish().await;
            let http = http.finish().await;
            assert!(outcome.is_err(), "malformed image reported vision success");
            assert_joined(&trace, 1, &http);
            assert!(trace.images.lock().unwrap().is_empty());
            assert!(http.requests.lock().unwrap().is_empty());
            assert!(trace.contexts.lock().unwrap()[0]
                .snapshot_rich()
                .unwrap()
                .messages
                .is_empty());
        }
    }
    #[tokio::test]
    async fn lowered_native_media_and_unchanged_nonimage_json_budgets_fail_closed() {
        for image_budget in [true, false] {
            let http = Http::start().await;
            let mut limits = ImageInputLimits::default();
            if image_budget {
                limits.max_native_frame_bytes = 1024;
            }
            let host = Host::start_with(
                ImageBot::new(http.url.clone()).unwrap(),
                config(),
                Some(limits),
            )
            .await;
            let mut socket = connect(&host.url).await.unwrap();
            let outcome: Result<String, Error> = async {
                register(&socket, &png_rgb(32, 32, [24, 80, 140]), "image/png").await?;
                let oversized_text = "x".repeat(140 * 1024);
                text(
                    &socket,
                    if image_budget {
                        "too many raw bytes"
                    } else {
                        &oversized_text
                    },
                )
                .await?;
                response(&mut socket).await
            }
            .await;
            close(socket).await;
            let (_report, trace) = host.finish().await;
            let http = http.finish().await;
            assert!(outcome.is_err());
            assert_joined(&trace, 1, &http);
            assert!(http.requests.lock().unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn held_rgb_is_released_on_each_real_host_termination_and_all_owners_join() {
        use pipecat_evals::bot_host::EvalBotExit;
        let mut reports = Vec::new();
        for terminal in [
            "eval-cancel",
            "socket-close",
            "host-cancel",
            "session-timeout",
            "End",
        ] {
            let http = Http::start().await;
            let bot = ImageBot::new(http.url.clone()).unwrap();
            bot.trace.hold_next_image.store(true, Ordering::SeqCst);
            bot.trace
                .observe_held_image_close
                .store(terminal == "End", Ordering::SeqCst);
            let mut settings = config();
            if terminal == "session-timeout" {
                settings.session_timeout = std::time::Duration::from_millis(700);
            }
            let host = Host::start_with(bot, settings, Some(ImageInputLimits::default())).await;
            let mut socket = Some(connect(&host.url).await.unwrap());
            let outcome: Result<(), Error> = async {
                register(
                    socket.as_ref().unwrap(),
                    &png_rgb(16, 12, [24, 80, 140]),
                    "image/png",
                )
                .await?;
                text(socket.as_ref().unwrap(), "held actual RGB at teardown").await?;
                timeout(WAIT, host.trace.image_arrived.notified())
                    .await
                    .map_err(|error| format!("{terminal}: waiting for held RGB: {error}"))?;
                assert_eq!(host.trace.held_images.load(Ordering::SeqCst), 1);
                assert!(http.trace.requests.lock().unwrap().is_empty());
                match terminal {
                    "eval-cancel" => {
                        send(
                            socket.as_ref().unwrap(),
                            serializer::control(8, serializer::EVAL_CANCEL, json!({})),
                        )
                        .await?
                    }
                    "socket-close" => close(socket.take().unwrap()).await,
                    "host-cancel" => host.cancel.cancel(),
                    "session-timeout" => {}
                    "End" => {
                        let constraint = host
                            .trace
                            .held_image_constraint
                            .lock()
                            .unwrap()
                            .take()
                            .ok_or("End: missing genuine held RGB constraint")?;
                        let original_fence = host
                            .trace
                            .held_image_fence
                            .lock()
                            .unwrap()
                            .take()
                            .ok_or("End: missing original request fence")?;
                        assert!(constraint.is_current());
                        send(
                            socket.as_ref().unwrap(),
                            serializer::message(8, "disconnect-bot", json!({})),
                        )
                        .await?;
                        // This router's End waits behind its held System call.
                        // Upstream image-input prepare_end closes the source
                        // first: source-stale with a live pure causal fence is
                        // direct evidence of that closure, not a timeout guess.
                        timeout(WAIT, async {
                            loop {
                                if !original_fence.is_current() {
                                    return Err("End: original request retired before source-close observation");
                                }
                                if !constraint.is_current() {
                                    if !original_fence.is_current() {
                                        return Err("End: source-close observation raced causal retirement");
                                    }
                                    return Ok(());
                                }
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .map_err(|error| format!("End: waiting for real image source closure: {error}"))??;
                        // The observation clone shares the real storage lease.
                        // Drop it before release/join so it cannot retain a slot.
                        drop(constraint);
                        drop(original_fence);
                        host.trace.image_gate.add_permits(1);
                    }
                    _ => unreachable!(),
                }
                // Cancellation cases never release their gate: native shutdown
                // must drop the held future/frame/lease. Graceful End drains the
                // released frame only after source admission has been closed.
                timeout(WAIT, host.trace.cleanup_arrived.notified())
                    .await
                    .map_err(|error| format!("{terminal}: waiting for app cleanup: {error}"))?;
                Ok(())
            }
            .await;
            drop(host.trace.held_image_constraint.lock().unwrap().take());
            if let Some(socket) = socket.take() {
                close(socket).await;
            }
            let (report, trace) = host.finish().await;
            let http = http.finish().await;
            assert_joined(&trace, 1, &http);
            assert_eq!(
                trace.held_images.load(Ordering::SeqCst),
                0,
                "{terminal} retained a genuine System image call"
            );
            assert_eq!(trace.assistant_image_calls.load(Ordering::SeqCst), 0);
            assert!(trace.contexts.lock().unwrap()[0]
                .snapshot_rich()
                .unwrap()
                .messages
                .is_empty());
            assert!(http.requests.lock().unwrap().is_empty());
            reports.push((terminal, outcome, report));
        }
        // Finish every real teardown and retain every observed exit before an
        // expectation fails, so one terminal cannot hide the remaining matrix.
        eprintln!("held RGB termination results: {reports:?}");
        for (terminal, outcome, report) in reports {
            outcome.unwrap_or_else(|error| panic!("{terminal}: {error}"));
            if matches!(
                terminal,
                "eval-cancel" | "socket-close" | "host-cancel" | "session-timeout"
            ) {
                // Cancel queues behind this deliberately held System call.
                // The pipeline's own deadline aborts and joins its actors;
                // carrier release above proves cleanup, not cooperative Cancel.
                assert_eq!(
                    report.as_ref().err().map(String::as_str),
                    Some("pipeline shutdown timed out"),
                    "{terminal}: {report:?}",
                );
            } else {
                let report = report.unwrap_or_else(|error| panic!("{terminal}: {error}"));
                assert_eq!(report.sessions, 1);
                assert_eq!(terminal, "End");
                assert_eq!(report.last_exit, Some(EvalBotExit::Ended));
            }
        }
    }

    #[tokio::test]
    async fn actual_passive_request_proves_send_after_keeps_a_until_b_is_admitted() {
        for fail_wait in [false, true] {
            let bundle = Bundle::new();
            bundle.put("a.png", png_rgb(4, 4, [24, 80, 140]));
            bundle.put("b.png", png_rgb(4, 4, [150, 30, 70]));
            let after = if fail_wait {
                json!({"event":"bot_started_speaking"})
            } else {
                json!({"delay_ms":400})
            };
            let mut turns = vec![
                json!({"image":"a.png","user":"first image","expect":[{"event":"response","text_contains":ANSWER}]}),
                json!({"image":"b.png","user":"scheduled replacement","send_after":after,"expect":[{"event":"response","text_contains":ANSWER}]}),
            ];
            if fail_wait {
                turns.push(json!({"user":"after failed wait","expect":[{"event":"response","text_contains":ANSWER}]}));
            }
            let path = bundle.json(
                "timing.json",
                &json!({"name":"source registration timing","stop_on_failure":false,"turns":turns}),
            );
            let http = Http::start().await;
            let bot = ImageBot::new(http.url.clone()).unwrap();
            // The task is launched only after the actual standard assistant
            // finishes the first provider response, then requests a passive
            // image while EvalSession is still in its send_after wait.
            *bot.trace.probe_after_response.lock().unwrap() =
                Some(std::time::Duration::from_millis(50));
            let host = Host::start(bot).await;
            let mut settings = options();
            settings.send_after_timeout = std::time::Duration::from_millis(300);
            let session = UserImageScenarioFile::load(path)
                .unwrap()
                .into_sessions(&host.url, settings)
                .unwrap()
                .remove(0);
            let result = session.run(Cancellation::new()).await;
            let (report, trace) = host.finish().await;
            let http = http.finish().await;
            assert!(report.is_ok(), "{report:?}");
            assert_joined(&trace, 1, &http);
            assert_eq!(result.passed, !fail_wait, "{result:?}");
            if fail_wait {
                assert!(result
                    .failures
                    .iter()
                    .any(|failure| failure.kind == "send_after_timeout"));
                assert_eq!(result.turns[2].status, "passed");
            }
            let images = trace.images.lock().unwrap();
            assert_eq!(images.len(), 3, "{result:?}");
            assert_eq!(images[0].text.as_deref(), Some("first image"));
            assert_eq!(images[1].text.as_deref(), Some("passive timing probe"));
            assert_eq!(
                images[1].rgb,
                rgb(4, 4, [24, 80, 140]),
                "B was registered before send_after admission"
            );
            assert_eq!(images[1].append_to_context, Some(false));
            let final_color = if fail_wait {
                [24, 80, 140]
            } else {
                [150, 30, 70]
            };
            assert_eq!(
                images[2].rgb,
                rgb(4, 4, final_color),
                "timed-out turn registered B or admitted turn failed to replace A"
            );
            assert_eq!(trace.assistant_image_calls.load(Ordering::SeqCst), 3);
            let requests = http.requests.lock().unwrap();
            assert_eq!(requests.len(), 2, "passive timing probe caused inference");
            assert_http_image(
                &requests[1],
                if fail_wait {
                    "after failed wait"
                } else {
                    "scheduled replacement"
                },
                (4, 4),
                &rgb(4, 4, final_color),
                2,
            );
        }
    }

    #[tokio::test]
    async fn owner_close_after_host_boundary_before_native_llm_launch_preserves_history_without_http(
    ) {
        use pipecat_evals::bot_host::EvalBotExit;
        let http = Http::start().await;
        let bot = ImageBot::new(http.url.clone()).unwrap();
        bot.trace.hold_next_context.store(true, Ordering::SeqCst);
        let host = Host::start(bot).await;
        let socket = connect(&host.url).await.unwrap();
        let outcome: Result<(), Error> = async {
            register(&socket, &png_rgb(4, 4, [24, 80, 140]), "image/png").await?;
            text(&socket, "committed before owner close").await?;
            timeout(WAIT, host.trace.context_arrived.notified()).await?;
            assert_eq!(
                host.trace.contexts.lock().unwrap()[0]
                    .snapshot_rich()
                    .unwrap()
                    .messages
                    .len(),
                1
            );
            assert!(http.trace.requests.lock().unwrap().is_empty());
            send(&socket, serializer::message(8, "disconnect-bot", json!({}))).await?;
            // This is downstream of UserImageInput.prepare_end, which closes
            // the exact session owner; no elapsed-time inference of closure.
            timeout(WAIT, host.trace.end_arrived.notified()).await?;
            host.trace.context_gate.add_permits(1);
            timeout(WAIT, host.trace.cleanup_arrived.notified()).await?;
            Ok(())
        }
        .await;
        host.trace.context_gate.add_permits(1);
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        outcome.unwrap();
        assert_eq!(report.unwrap().last_exit, Some(EvalBotExit::Ended));
        assert_joined(&trace, 1, &http);
        assert!(
            http.requests.lock().unwrap().is_empty(),
            "source-closed context launched actual native HTTP after boundary admission"
        );
        let context = trace.contexts.lock().unwrap()[0].snapshot_rich().unwrap();
        assert_eq!(context.messages.len(), 1);
        assert_eq!(context.messages[0].role, ChatRole::User);
        assert_eq!(trace.assistant_image_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn owner_close_cancels_started_native_http_peer_before_fixture_shutdown() {
        use pipecat_evals::bot_host::EvalBotExit;
        let http = Http::start().await;
        http.trace.gated.store(true, Ordering::SeqCst);
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let socket = connect(&host.url).await.unwrap();
        let outcome: Result<(), Error> = async {
            register(&socket, &png_rgb(4, 4, [24, 80, 140]), "image/png").await?;
            text(&socket, "already launched before owner close").await?;
            timeout(WAIT, http.trace.arrived.notified()).await?;
            assert_eq!(http.trace.active.load(Ordering::SeqCst), 1);
            send(&socket, serializer::message(8, "disconnect-bot", json!({}))).await?;
            timeout(WAIT, host.trace.end_arrived.notified()).await?;
            // Server remains alive and its response gate remains shut. Observe
            // real client EOF before server cancellation or socket teardown.
            timeout(
                std::time::Duration::from_secs(2),
                http.trace.peer_closed.notified(),
            )
            .await?;
            assert_eq!(http.trace.closed_connections.load(Ordering::SeqCst), 1);
            assert_eq!(http.trace.joined.load(Ordering::SeqCst), 0);
            timeout(WAIT, host.trace.cleanup_arrived.notified()).await?;
            Ok(())
        }
        .await;
        close(socket).await;
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        outcome.unwrap();
        assert_eq!(report.unwrap().last_exit, Some(EvalBotExit::Ended));
        assert_joined(&trace, 1, &http);
        assert_eq!(http.closed_connections.load(Ordering::SeqCst), 1);
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_http_image(
            &requests[0],
            "already launched before owner close",
            (4, 4),
            &rgb(4, 4, [24, 80, 140]),
            1,
        );
        let context = trace.contexts.lock().unwrap()[0].snapshot_rich().unwrap();
        assert_eq!(context.messages.len(), 1);
        assert_eq!(context.messages[0].role, ChatRole::User);
        // LlmClient's private provider task handle is not exposed. This proves
        // owned pipeline completion cleanup, observed peer cancellation and
        // fixture server/socket join, not an explicit provider JoinHandle await.
    }

    #[tokio::test]
    async fn waiting_for_real_provider_response_cannot_finish_at_socket_or_image_admission() {
        let bundle = Bundle::new();
        bundle.put("a.png", png_rgb(4, 4, [24, 80, 140]));
        let http = Http::start().await;
        http.trace.gated.store(true, Ordering::SeqCst);
        let host = Host::start(ImageBot::new(http.url.clone()).unwrap()).await;
        let session = UserImageScenarioFile::load(bundle.source("a.png", ANSWER))
            .unwrap()
            .into_sessions(&host.url, options())
            .unwrap()
            .remove(0);
        let task = tokio::spawn(session.run(Cancellation::new()));
        let arrived = timeout(WAIT, http.trace.arrived.notified()).await;
        let finished_before_provider = task.is_finished();
        http.trace.gate.add_permits(1);
        let result = task.await.unwrap();
        let (report, trace) = host.finish().await;
        let http = http.finish().await;
        arrived.unwrap();
        assert!(!finished_before_provider);
        assert!(result.passed, "{result:?}");
        assert!(report.is_ok());
        assert_joined(&trace, 1, &http);
        assert_eq!(http.requests.lock().unwrap().len(), 1);
    }
}
