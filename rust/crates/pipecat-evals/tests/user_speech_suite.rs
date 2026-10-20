//! Real suite CLI -> per-attempt compiled child -> EvalBotHost -> full PCM -> assertions.
//! Synthetic loopback providers only; no paid service. The combined-feature
//! joint module loads the already-installed local Silero model; speech-only tests do not.
#![cfg(unix)]
#[path = "support/hosted_user_speech.rs"]
mod fixture;
use fixture::*;
#[cfg(feature = "user-speech")]
use serde_json::{json, Value};
use tokio::net::TcpListener;

#[cfg(feature = "user-speech")]
#[tokio::test]
async fn user_speech_suite_bot() {
    fixture::enabled::child().await;
}

#[tokio::test]
async fn suite_flag_or_missing_joint_configuration_rejects_before_child_provider_or_cache_work() {
    let b = Bundle::new();
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache = b.cache();
    b.json("one.json", &source("one", &["reference"]));
    let manifest = b.json(
        "manifest.json",
        &manifest(vec![entry(&b, "one", "one.json", "unused")], 1, 1),
    );
    let runtime = b.json(
        "runtime.json",
        &runtime(
            &format!("http://{}", provider.local_addr().unwrap()),
            Some(&cache),
        ),
    );
    let base = arguments(&manifest, &runtime);
    for extra in [
        vec!["--unknown"],
        vec!["--user-speech", "other.json"],
        vec!["--recorded-audio"],
        // With both features, this is missing joint configuration, not an illegal flag pair.
        vec!["--bot-audio-response", "other.json"],
    ] {
        let mut args = base.clone();
        args.extend(extra.into_iter().map(str::to_owned));
        let output = cli(&args, &b.0).await;
        assert_exit(&output, 2);
        assert!(output.stdout.is_empty());
        assert!(b.markers("started-").is_empty());
        assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
    }
    let output = cli(&base[..3], &b.0).await;
    assert_exit(&output, 2);
    assert!(output.stdout.is_empty());
    assert_idle(&provider).await;
}

#[cfg(not(feature = "user-speech"))]
#[tokio::test]
async fn feature_disabled_suite_cli_rejects_before_runtime_or_credential_access() {
    let b = Bundle::new();
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache = b.cache();
    b.json("one.json", &source("one", &["reference"]));
    let manifest = b.json(
        "manifest.json",
        &manifest(vec![entry(&b, "one", "one.json", "unused")], 1, 1),
    );
    // Neither runtime nor credential resolution may supersede the feature rejection.
    let runtime = b.put("runtime.json", b"this must never be parsed as runtime JSON");
    let config = b.put(
        "stt-runtime.json",
        b"this must never be parsed as transcription JSON",
    );
    let mut args = arguments(&manifest, &runtime);
    args.extend(["--bot-audio-response".into(), config.display().to_string()]);
    let output = cli(&args, &b.0).await;
    assert_exit(&output, 2);
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("user-speech"));
    assert!(b.markers("started-").is_empty());
    assert_eq!(std::fs::read_dir(&cache).unwrap().count(), 0);
    assert_idle(&provider).await;
}

#[cfg(feature = "user-speech")]
mod enabled {
    use super::*;
    use fixture::enabled::*;
    use pipecat_evals::{
        process::ProcessOptions,
        release::{run_release, run_release_with_budget, ReleaseManifest, ReleaseResult},
        Cancellation, EvalError, SessionOptions,
    };
    use std::{sync::atomic::Ordering, time::Duration};

    fn options() -> SessionOptions {
        SessionOptions {
            default_timeout: Duration::from_secs(3),
            handshake_timeout: Duration::from_secs(2),
            max_duration: WAIT,
            ..Default::default()
        }
    }
    fn process_options() -> ProcessOptions {
        ProcessOptions {
            startup_timeout: Duration::from_secs(2),
            shutdown_grace: Duration::from_millis(500),
            output_drain_timeout: Duration::from_millis(500),
            ..Default::default()
        }
    }
    async fn run(plan: ReleaseManifest, cancel: Cancellation) -> Result<ReleaseResult, EvalError> {
        let future = run_release(plan, options(), process_options(), cancel.clone());
        tokio::pin!(future);
        match tokio::time::timeout(WAIT, &mut future).await {
            Ok(result) => result,
            Err(_) => {
                cancel.cancel();
                let result = future.await;
                panic!("release deadline after cooperative actual join: {result:?}");
            }
        }
    }
    fn value(result: &ReleaseResult) -> Value {
        serde_json::to_value(result).unwrap()
    }
    fn failures(result: &ReleaseResult) -> Vec<&str> {
        result
            .runs
            .iter()
            .flat_map(|r| r.hosted.result.failures.iter().map(|f| f.kind.as_str()))
            .collect()
    }
    fn one(b: &Bundle, clips: &[Vec<u8>], name: &str, mode: &str) -> std::path::PathBuf {
        profile(b, name, clips, &format!("isolated {name}"), mode, 0);
        b.json(
            &format!("{name}.json"),
            &source(name, &["exact fixture reference"]),
        );
        b.json(
            "manifest.json",
            &manifest(vec![entry(b, name, &format!("{name}.json"), name)], 1, 1),
        )
    }
    fn assert_joined(result: &ReleaseResult) {
        assert_reaped(&value(result));
        assert!(!format!("{result:?}").contains(SECRET));
    }
    async fn no_parent_tasks() {
        tokio::time::timeout(Duration::from_secs(1), async {
            while tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks()
                != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("selected release returned with spawned playback/socket/pipe task still alive");
    }

    #[tokio::test]
    async fn actual_cli_repeat_two_labels_requires_complete_pcm_and_second_turn_in_attempt_order() {
        let b = Bundle::new();
        let speech = pcm(17);
        let slow_wav = pcm(37);
        let fast_wav = pcm(57);
        b.put("slow.wav", wav(&slow_wav));
        b.put("fast.wav", wav(&fast_wav));
        for (name, recording, wait_delta) in [("slow", &slow_wav, 1), ("fast", &fast_wav, 0)] {
            let mut scenario = source(
                name,
                &[
                    "shared literal reference",
                    "WAV reference must not synthesize",
                ],
            );
            scenario["turns"][1]["audio"] = format!("{name}.wav").into();
            // Expectations consume the event stream in order. The fixture
            // emits the function call before its generated-text response.
            scenario["turns"][1]["expect"].as_array_mut().unwrap().insert(
                0,
                json!({"event":"function_call","calls":[{"name":"complete_pcm"}],"within_ms":3000}),
            );
            b.json(&format!("{name}.json"), &scenario);
            profile(
                &b,
                name,
                &[speech.clone(), recording.clone()],
                &format!("isolated {name}"),
                "normal",
                wait_delta,
            );
        }
        let spec = manifest(
            vec![
                entry(&b, "slow-label", "slow.json", "slow"),
                entry(&b, "fast-label", "fast.json", "fast"),
            ],
            2,
            2,
        );
        let base = spec["base_port"].as_u64().unwrap();
        let path = b.json("manifest.json", &spec);
        let http = Http::start(vec![Reply::pcm(speech.clone()); 4]).await;
        let runtime = b.json("runtime.json", &runtime(&http.url, None));
        let output = cli(&arguments(&path, &runtime), &b.0).await;
        let trace = http.finish().await;
        assert_exit(&output, 0);
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["passed"], true);
        assert_eq!(
            result["runs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| (
                    r["attempt"].as_u64().unwrap(),
                    r["name"].as_str().unwrap(),
                    r["scenario"].as_str().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, "slow-label", "slow"),
                (1, "fast-label", "fast"),
                (2, "slow-label", "slow"),
                (2, "fast-label", "fast")
            ]
        );
        assert_consumed(
            &b,
            &result,
            &[
                vec![speech.clone(), slow_wav.clone()],
                vec![speech.clone(), fast_wav.clone()],
                vec![speech.clone(), slow_wav],
                vec![speech, fast_wav],
            ],
        );
        let order = std::fs::read_to_string(b.0.join("completion-order"))
            .unwrap()
            .lines()
            .map(|s| s.parse::<u64>().unwrap())
            .collect::<Vec<_>>();
        for offset in [0, 2] {
            assert!(
                order.iter().position(|p| *p == base + offset + 1)
                    < order.iter().position(|p| *p == base + offset)
            );
        }
        assert_eq!(b.markers("started-").len(), 4);
        assert_eq!(trace.requests.lock().unwrap().len(), 4);
        assert!(trace
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r["transcript"] == "shared literal reference"));
        assert_eq!(trace.joined.load(Ordering::SeqCst), 4);
        assert!(
            b.markers("cache").is_empty(),
            "no-cache route performed implicit cache I/O"
        );
    }

    #[tokio::test]
    async fn cli_cannot_finish_on_sender_admission_before_child_consumes_full_tail() {
        let b = Bundle::new();
        let bytes = pcm(67);
        let path = one(&b, std::slice::from_ref(&bytes), "gated", "gated");
        let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
        let runtime = b.json("runtime.json", &runtime(&http.url, None));
        let args = arguments(&path, &runtime);
        let cwd = b.0.clone();
        let mut cli_task = tokio::spawn(async move { cli(&args, &cwd).await });
        let observed = tokio::time::timeout(Duration::from_secs(5), wait_for(&b, "tail-1-")).await;
        let was_finished = cli_task.is_finished();
        let consumed = b.markers("consumed-").len();
        for started in b.markers("started-") {
            let port = started
                .file_name()
                .unwrap()
                .to_string_lossy()
                .trim_start_matches("started-")
                .to_owned();
            b.put(&format!("release-{port}"), b"go");
        }
        let output = (&mut cli_task).await.unwrap();
        let _ = http.finish().await;
        assert!(observed.is_ok());
        assert!(
            !was_finished,
            "CLI reported sender admission as complete child consumption"
        );
        assert_eq!(consumed, 0);
        assert_exit(&output, 0);
        let result = serde_json::from_slice(&output.stdout).unwrap();
        assert_consumed(&b, &result, &[vec![bytes]]);
    }

    #[tokio::test]
    async fn cold_and_warm_cli_repeats_share_cache_ownership_but_each_child_consumes_pcm() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(77);
        let mut entries = vec![];
        for name in ["first", "second"] {
            profile(
                &b,
                name,
                std::slice::from_ref(&bytes),
                &format!("isolated {name}"),
                "normal",
                0,
            );
            b.json(
                &format!("{name}.json"),
                &source(name, &["same cache reference"]),
            );
            entries.push(entry(&b, name, &format!("{name}.json"), name));
        }
        let mut spec = manifest(entries, 1, 2);
        let path = b.json("manifest.json", &spec);
        let mut reply = Reply::pcm(bytes.clone());
        reply.delay = Duration::from_millis(150);
        let http = Http::start(vec![reply]).await;
        let runtime = b.json("runtime.json", &runtime(&http.url, Some(&root)));
        let cold = cli(&arguments(&path, &runtime), &b.0).await;
        let trace = http.finish().await;
        assert_exit(&cold, 0);
        assert_consumed(
            &b,
            &serde_json::from_slice(&cold.stdout).unwrap(),
            &[vec![bytes.clone()], vec![bytes.clone()]],
        );
        assert_eq!(
            trace.requests.lock().unwrap().len(),
            1,
            "concurrent same-key users duplicated provider work"
        );
        let files = cache_wavs(&root);
        assert_eq!(files.len(), 1);
        assert_eq!(
            pipecat_evals::RecordedAudio::from_wav(&std::fs::read(&files[0]).unwrap())
                .unwrap()
                .pcm_bytes(),
            bytes
        );
        assert_no_temporaries(&root);
        assert_cache_unlocked(&root);
        // Reuse the now-closed provider URL in a fresh actual CLI process.
        spec["base_port"] = manifest(vec![], 1, 1)["base_port"].clone();
        b.json("manifest.json", &spec);
        let warm = cli(&arguments(&path, &runtime), &b.0).await;
        assert_exit(&warm, 0);
        assert_consumed(
            &b,
            &serde_json::from_slice(&warm.stdout).unwrap(),
            &[vec![bytes.clone()], vec![bytes]],
        );
        assert_no_temporaries(&root);
        assert_cache_unlocked(&root);
    }

    #[tokio::test]
    async fn whole_block_voice_override_include_and_wav_precedence_reach_real_host() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(87);
        let recording = pcm(97);
        b.put("override.wav", wav(&recording));
        b.put("turns.yaml",b"- user: Include CASE reference\n  expect:\n    - event: llm_response\n      text_contains: complete PCM 1\n      within_ms: 3000\n- user: WAV reference never synthesized\n  audio: override.wav\n  expect:\n    - event: llm_response\n      text_contains: complete PCM 2\n      within_ms: 3000\n");
        b.put("source.yaml",b"name: bundle\nuser:\n  modality: audio\n  speech: {service: cartesia, voice: inherited-voice, model: inherited-model, language: es}\nscenarios:\n  - name: override\n    user:\n      modality: audio\n      speech: {service: cartesia, voice: Override-VOICE}\n    context: [{role: system, content: isolated override}]\n    turns: !include turns.yaml\n");
        profile(
            &b,
            "override",
            &[bytes.clone(), recording.clone()],
            "isolated override",
            "normal",
            0,
        );
        let path = b.json(
            "manifest.json",
            &manifest(vec![entry(&b, "override", "source.yaml", "override")], 1, 1),
        );
        let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
        let runtime = b.json("runtime.json", &runtime(&http.url, Some(&root)));
        let output = cli(&arguments(&path, &runtime), &b.0).await;
        let trace = http.finish().await;
        assert_exit(&output, 0);
        assert_consumed(
            &b,
            &serde_json::from_slice(&output.stdout).unwrap(),
            &[vec![bytes, recording]],
        );
        let requests = trace.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["voice"]["id"], "Override-VOICE");
        assert_eq!(requests[0]["model_id"], "sonic-2");
        assert_eq!(requests[0]["language"], "en");
        assert_eq!(requests[0]["transcript"], "Include CASE reference");
    }

    #[tokio::test]
    async fn voice_model_language_each_distinguish_cache_keys() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(107);
        let mut entries = vec![];
        for (name, voice, model, language) in [
            ("base", "Voice-A", "sonic-2", "en"),
            ("voice", "Voice-B", "sonic-2", "en"),
            ("model", "Voice-A", "sonic-turbo", "en"),
            ("language", "Voice-A", "sonic-2", "es"),
        ] {
            let mut s = source(name, &["same case-sensitive reference"]);
            s["user"]["speech"] =
                json!({"service":"cartesia","voice":voice,"model":model,"language":language});
            b.json(&format!("{name}.json"), &s);
            profile(
                &b,
                name,
                std::slice::from_ref(&bytes),
                &format!("isolated {name}"),
                "normal",
                0,
            );
            entries.push(entry(&b, name, &format!("{name}.json"), name));
        }
        let path = b.json("manifest.json", &manifest(entries, 1, 2));
        let http = Http::start(vec![Reply::pcm(bytes.clone()); 4]).await;
        let result = run(
            ReleaseManifest::load_user_speech(path, settings(&http.url, Some(&root))).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
        let trace = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert_consumed(&b, &value(&result), &vec![vec![bytes]; 4]);
        assert_eq!(cache_wavs(&root).len(), 4);
        assert_eq!(trace.requests.lock().unwrap().len(), 4);
        assert_cache_unlocked(&root);
    }

    #[tokio::test]
    async fn all_wav_selected_route_never_creates_speech_http_or_cache_owner() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(117);
        b.put("only.wav", wav(&bytes));
        let path = one(&b, std::slice::from_ref(&bytes), "wav", "normal");
        let mut s = source("wav", &["unused speech reference"]);
        s["user"] = json!({"modality":"audio"});
        s["turns"][0]["audio"] = "only.wav".into();
        b.json("wav.json", &s);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let result = run(
            ReleaseManifest::load_user_speech(
                path,
                settings(
                    &format!("http://{}", listener.local_addr().unwrap()),
                    Some(&root),
                ),
            )
            .unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
        assert!(result.passed, "{result:?}");
        assert_consumed(&b, &value(&result), &[vec![bytes]]);
        assert_idle(&listener).await;
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
        no_parent_tasks().await;
    }

    fn precharged(
        b: &Bundle,
        name: &str,
        bytes: &[u8],
        wav_bytes: usize,
        repeat: usize,
    ) -> std::path::PathBuf {
        assert!(wav_bytes.is_multiple_of(2));
        let mut s = source(name, &["exact fixture reference"]);
        let turns = s["turns"].as_array_mut().unwrap();
        let mut remaining = wav_bytes;
        let mut n = 0;
        while remaining != 0 {
            let size = remaining.min(1_920_000);
            let file = format!("{name}-future-{n}.wav");
            b.put(&file, wav(&[9, 1].repeat(size / 2)));
            turns.push(json!({"user":"future WAV already conservatively charged","audio":file,"expect":[{"event":"llm_response","text_contains":"future filler must never be reached","within_ms":25}]}));
            remaining -= size;
            n += 1;
        }
        b.json(&format!("{name}.json"), &s);
        profile(
            b,
            name,
            &[bytes.to_vec()],
            &format!("isolated {name}"),
            "normal",
            0,
        );
        b.json(
            &format!("{name}-manifest.json"),
            &manifest(
                vec![entry(b, name, &format!("{name}.json"), name)],
                repeat,
                1,
            ),
        )
    }
    async fn prime(
        b: &Bundle,
        bytes: &[u8],
    ) -> (
        pipecat_evals::CartesiaUserSpeechSettings,
        std::path::PathBuf,
    ) {
        let root = b.cache();
        let path = one(b, &[bytes.to_vec()], "prime", "normal");
        let http = Http::start(vec![Reply::pcm(bytes.to_vec())]).await;
        let configured = settings(&http.url, Some(&root));
        let result = run(
            ReleaseManifest::load_user_speech(path, configured.clone()).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
        let trace = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert_consumed(b, &value(&result), &[vec![bytes.to_vec()]]);
        assert_eq!(trace.requests.lock().unwrap().len(), 1);
        (configured, root)
    }
    #[tokio::test]
    async fn warm_hit_crosses_exact_15360000_budget_before_any_pcm_for_repeat_and_combined_plans() {
        let prime_bundle = Bundle::new();
        let bytes = pcm(127);
        // All three modes reserve the same logical WAV total. Divisibility by
        // four makes each half a complete PCM16 payload; the warm clip exceeds
        // the shared remainder by exactly one sample, never a rounded budget.
        let total_precharged_wav_bytes = PCM_BUDGET - bytes.len() + 2;
        assert!(total_precharged_wav_bytes.is_multiple_of(4));
        assert_eq!(PCM_BUDGET - total_precharged_wav_bytes, bytes.len() - 2);
        let (configured, root) = prime(&prime_bundle, &bytes).await;
        let before = std::fs::read(&cache_wavs(&root)[0]).unwrap();
        for mode in ["single", "repeat", "combined"] {
            let b = Bundle::new();
            let plan = match mode {
                "single" => ReleaseManifest::load_user_speech(
                    precharged(&b, "single", &bytes, total_precharged_wav_bytes, 1),
                    configured.clone(),
                )
                .unwrap(),
                "repeat" => ReleaseManifest::load_user_speech(
                    precharged(&b, "repeat", &bytes, total_precharged_wav_bytes / 2, 2),
                    configured.clone(),
                )
                .unwrap(),
                _ => {
                    let mut first = ReleaseManifest::load_user_speech(
                        precharged(&b, "first", &bytes, total_precharged_wav_bytes / 2, 1),
                        configured.clone(),
                    )
                    .unwrap();
                    let second = ReleaseManifest::load_user_speech(
                        precharged(&b, "second", &bytes, total_precharged_wav_bytes / 2, 1),
                        configured.clone(),
                    )
                    .unwrap();
                    first.jobs.extend(second.jobs);
                    first
                }
            };
            // The WAV charge is logical future use. None of this filler is played.
            let result = run(plan, Cancellation::new()).await.unwrap();
            assert!(!result.passed, "{mode}: {result:?}");
            assert_joined(&result);
            assert!(
                result.runs.iter().all(|r| r
                    .hosted
                    .result
                    .failures
                    .iter()
                    .any(|f| f.kind == "limit_exceeded" && f.reason.contains("PCM budget"))),
                "{mode}: {result:?}"
            );
            assert_no_audio(&b);
            assert_eq!(
                b.markers("started-").len(),
                if mode == "single" { 1 } else { 2 }
            );
            assert_eq!(std::fs::read(&cache_wavs(&root)[0]).unwrap(), before);
            assert_no_temporaries(&root);
            assert_cache_unlocked(&root);
            no_parent_tasks().await;
        }
    }

    #[tokio::test]
    async fn exact_pcm_reference_and_expanded_scenario_bounds_charge_every_logical_clone() {
        let b = Bundle::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let configured = settings(&format!("http://{}", listener.local_addr().unwrap()), None);
        b.put("maximum.wav", wav(&[1, 2].repeat(960_000)));
        b.put("sample.wav", wav(&[1, 2]));
        let mut s = source("maximum", &["reference"]);
        s["turns"][0]["audio"] = "maximum.wav".into();
        b.json("maximum.json", &s);
        let path = b.json(
            "manifest.json",
            &manifest(vec![entry(&b, "maximum", "maximum.json", "unused")], 8, 1),
        );
        let exact = ReleaseManifest::load_user_speech(&path, configured.clone()).unwrap();
        assert_eq!(exact.jobs.len(), 8);
        let mut over = exact;
        let mut clone = over.jobs[0].clone();
        clone.attempt = 9;
        clone.port += 8;
        over.jobs.push(clone);
        assert!(
            run(over, Cancellation::new()).await.is_err(),
            "Arc-shared recording clones escaped logical charge"
        );
        let mut tiny = source("tiny", &["reference"]);
        tiny["turns"][0]["audio"] = "sample.wav".into();
        b.json("tiny.json", &tiny);
        // Exactly cap plus one complete PCM16 sample, before any child.
        let exact = ReleaseManifest::load_user_speech(&path, configured.clone()).unwrap();
        let tiny_path = b.json(
            "tiny-manifest.json",
            &manifest(vec![entry(&b, "tiny", "tiny.json", "unused")], 1, 1),
        );
        let mut over = exact;
        // Ordinary recorded sources have no user.speech settings.
        tiny["user"] = json!({"modality":"audio"});
        b.json("tiny.json", &tiny);
        let recorded = ReleaseManifest::load_recorded(&tiny_path).unwrap();
        over.jobs.extend(recorded.jobs);
        assert!(run(over, Cancellation::new()).await.is_err());
        let mut refs = source("references", &["x"]);
        refs["turns"][0]["user"] = "x".repeat(16 * 1024).into();
        b.json("refs.json", &refs);
        let refs_path = b.json(
            "refs-manifest.json",
            &manifest(vec![entry(&b, "refs", "refs.json", "unused")], 128, 1),
        );
        assert_eq!(
            ReleaseManifest::load_user_speech(&refs_path, configured.clone())
                .unwrap()
                .jobs
                .len(),
            128
        );
        refs["turns"][0]["user"] = "x".repeat(16 * 1024 + 1).into();
        b.json("refs.json", &refs);
        assert!(ReleaseManifest::load_user_speech(&refs_path, configured.clone()).is_err());
        refs["turns"][0]["user"] = "x".into();
        refs["turns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"user":"second"}));
        b.json("refs.json", &refs);
        assert!(ReleaseManifest::load_user_speech(&refs_path, configured.clone()).is_err());
        // Exact 4 MiB expanded normalized scenarios, then one byte per occurrence over.
        let mut sized = source("sized", &["x"]);
        sized["context"][0]["content"] = "".into();
        b.json("sized.json", &sized);
        let sized_entry = entry(&b, "sized", "sized.json", "unused");
        let small = b.json(
            "small-manifest.json",
            &manifest(vec![sized_entry.clone()], 1, 1),
        );
        let small = ReleaseManifest::load_user_speech(small, configured.clone()).unwrap();
        let overhead = serde_json::to_vec(&small.jobs[0].scenario).unwrap().len();
        assert!(overhead < 32768);
        sized["context"][0]["content"] = "s".repeat(32768 - overhead).into();
        b.json("sized.json", &sized);
        let full = b.json("sized-manifest.json", &manifest(vec![sized_entry], 128, 1));
        assert_eq!(
            ReleaseManifest::load_user_speech(&full, configured.clone())
                .unwrap()
                .jobs
                .len(),
            128
        );
        sized["context"][0]["content"] = "s".repeat(32769 - overhead).into();
        b.json("sized.json", &sized);
        assert!(ReleaseManifest::load_user_speech(full, configured).is_err());
        assert!(b.markers("started-").is_empty());
        assert_idle(&listener).await;
    }

    #[tokio::test]
    async fn last_invalid_job_dtmf_and_every_public_scenario_mutation_have_zero_side_effects() {
        let b = Bundle::new();
        let root = b.cache();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let configured = settings(
            &format!("http://{}", listener.local_addr().unwrap()),
            Some(&root),
        );
        b.json(
            "good.json",
            &source("good", &["first reference", "second reference"]),
        );
        let path = b.json(
            "manifest.json",
            &manifest(vec![entry(&b, "good", "good.json", "unused")], 1, 1),
        );
        for mutation in [
            "reference",
            "expectation",
            "context",
            "name",
            "order",
            "append",
            "dtmf",
            "duplicate",
        ] {
            let mut plan = ReleaseManifest::load_user_speech(&path, configured.clone()).unwrap();
            let s = &mut plan.jobs[0].scenario;
            match mutation {
                "reference" => s.turns[0].user = Some("changed after load".into()),
                "expectation" => s.turns[0].expect[0].text_contains = Some("changed".into()),
                "context" => {
                    s.context[0].insert("content".into(), "changed".into());
                }
                "name" => s.name = "changed".into(),
                "order" => s.turns.swap(0, 1),
                "append" => s.turns.push(s.turns[0].clone()),
                "dtmf" => {
                    s.turns[0].user = None;
                    s.turns[0].dtmf = Some("1".into());
                }
                _ => {
                    plan.jobs.push(plan.jobs[0].clone());
                }
            }
            assert!(
                run(plan, Cancellation::new()).await.is_err(),
                "public {mutation} mutation was accepted"
            );
        }
        let mut bad = source("bad", &["valid reference"]);
        bad["turns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"dtmf":"1"}));
        b.json("bad.json", &bad);
        let invalid = b.json(
            "invalid.json",
            &manifest(
                vec![
                    entry(&b, "good", "good.json", "unused"),
                    entry(&b, "bad", "bad.json", "unused"),
                ],
                1,
                2,
            ),
        );
        assert!(ReleaseManifest::load_user_speech(&invalid, configured.clone()).is_err());
        let runtime = b.json(
            "runtime.json",
            &runtime(
                &format!("http://{}", listener.local_addr().unwrap()),
                Some(&root),
            ),
        );
        let output = cli(&arguments(&invalid, &runtime), &b.0).await;
        assert_exit(&output, 2);
        assert!(output.stdout.is_empty());
        bad["turns"] = json!([{"user":"missing recorded source","audio":"missing.wav"}]);
        b.json("bad.json", &bad);
        let output = cli(&arguments(&invalid, &runtime), &b.0).await;
        assert_exit(&output, 2);
        assert!(output.stdout.is_empty());
        let plan = ReleaseManifest::load_user_speech(path, configured).unwrap();
        let result =
            run_release_with_budget(plan, options(), process_options(), Cancellation::new(), 1)
                .await;
        assert!(result.is_err());
        assert!(b.markers("started-").is_empty());
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
        assert_idle(&listener).await;
    }

    #[tokio::test]
    async fn valid_public_job_reorder_and_new_identity_clone_revalidate_and_run() {
        let b = Bundle::new();
        let bytes = pcm(137);
        let path = one(&b, std::slice::from_ref(&bytes), "valid", "normal");
        let http = Http::start(vec![Reply::pcm(bytes.clone()); 2]).await;
        let mut plan = ReleaseManifest::load_user_speech(path, settings(&http.url, None)).unwrap();
        let mut clone = plan.jobs[0].clone();
        clone.name = "new-label".into();
        clone.attempt = 2;
        clone.port += 1;
        clone.concurrency = 2;
        plan.jobs.push(clone);
        plan.jobs.reverse();
        plan.concurrency = 2;
        let result = run(plan, Cancellation::new()).await.unwrap();
        let trace = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert_eq!(result.runs[0].name, "new-label");
        assert_consumed(&b, &value(&result), &[vec![bytes.clone()], vec![bytes]]);
        assert_eq!(trace.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn provider_failures_never_play_partial_audio_and_return_only_after_cleanup() {
        for mode in ["non-200", "partial", "stalled", "oversized"] {
            let b = Bundle::new();
            let root = b.cache();
            let bytes = pcm(147);
            let path = one(&b, std::slice::from_ref(&bytes), "failure", "normal");
            let mut reply = Reply::pcm(bytes);
            match mode {
                "non-200" => reply.status = 206,
                "partial" => reply.truncated = true,
                "stalled" => reply.stalled = true,
                _ => reply.body = vec![1; 1_920_002],
            }
            let http = Http::start(vec![reply]).await;
            let runtime = b.json("runtime.json", &runtime(&http.url, Some(&root)));
            let output = cli(&arguments(&path, &runtime), &b.0).await;
            let trace = http.finish().await;
            assert_exit(&output, 1);
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["passed"], false);
            assert_reaped(&result);
            assert_no_audio(&b);
            assert_eq!(trace.requests.lock().unwrap().len(), 1);
            assert_eq!(trace.joined.load(Ordering::SeqCst), 1);
            assert!(cache_wavs(&root).is_empty());
            assert_no_temporaries(&root);
            assert_cache_unlocked(&root);
        }
    }

    #[tokio::test]
    async fn corrupt_and_quota_limited_cache_fail_without_unsettled_publication() {
        let prime_bundle = Bundle::new();
        let bytes = pcm(157);
        let (configured, root) = prime(&prime_bundle, &bytes).await;
        let cached = cache_wavs(&root).remove(0);
        std::fs::write(&cached, b"not a WAV").unwrap();
        let b = Bundle::new();
        let path = one(&b, std::slice::from_ref(&bytes), "corrupt", "normal");
        let result = run(
            ReleaseManifest::load_user_speech(path, configured).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
        assert!(!result.passed);
        assert!(failures(&result).contains(&"user_speech_error"));
        assert_joined(&result);
        assert_no_audio(&b);
        assert_cache_unlocked(&root);
        let b = Bundle::new();
        let root = b.cache();
        let path = one(&b, std::slice::from_ref(&bytes), "quota", "normal");
        let http = Http::start(vec![Reply::pcm(bytes)]).await;
        let mut runtime_value = runtime(&http.url, Some(&root));
        runtime_value["cache_max_bytes"] = 1.into();
        let runtime = b.json("runtime.json", &runtime_value);
        let output = cli(&arguments(&path, &runtime), &b.0).await;
        let _ = http.finish().await;
        assert_exit(&output, 1);
        assert_reaped(&serde_json::from_slice(&output.stdout).unwrap());
        assert!(cache_wavs(&root).is_empty());
        assert_no_temporaries(&root);
        assert_cache_unlocked(&root);
    }

    #[tokio::test]
    async fn child_startup_crash_and_output_overflow_reap_before_failed_result() {
        for mode in ["never-ready", "crash", "flood"] {
            let b = Bundle::new();
            let bytes = pcm(167);
            let path = one(&b, std::slice::from_ref(&bytes), mode, mode);
            let http = Http::start(vec![Reply::pcm(bytes)]).await;
            let plan = ReleaseManifest::load_user_speech(path, settings(&http.url, None)).unwrap();
            let result = run(plan, Cancellation::new()).await.unwrap();
            let _ = http.finish().await;
            assert!(!result.passed, "{mode}: {result:?}");
            assert_joined(&result);
            assert_eq!(b.markers("started-").len(), 1);
            if mode == "flood" {
                assert!(result.runs[0].hosted.process.output_overflow);
            }
            assert!(
                !result.runs[0].hosted.process.output_incomplete,
                "pipe task was dropped instead of joined"
            );
            no_parent_tasks().await;
        }
    }

    #[tokio::test]
    async fn borrowed_release_timeout_retains_owner_then_cancel_reawait_joins_http_and_child() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(177);
        let path = one(&b, std::slice::from_ref(&bytes), "cancel", "normal");
        let mut spec: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        spec["repeat"] = 2.into();
        b.json("manifest.json", &spec);
        let mut reply = Reply::pcm(bytes);
        reply.stalled = true;
        let http = Http::start(vec![reply]).await;
        let mut config = pipecat_speech::SpeechConfig::cartesia(SECRET);
        config.base_url = http.url.clone();
        config.timeout = Duration::from_secs(5);
        let configured = pipecat_evals::CartesiaUserSpeechSettings::new(
            config,
            pipecat_evals::UserSpeechCachePolicy::Private {
                directory: root.clone(),
                max_entries: 8,
                max_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap();
        let plan = ReleaseManifest::load_user_speech(path, configured).unwrap();
        let cancel = Cancellation::new();
        let future = run_release(plan, options(), process_options(), cancel.clone());
        tokio::pin!(future);
        let borrowed = tokio::time::timeout(Duration::from_millis(400), &mut future).await;
        let timed_out = borrowed.is_err();
        let request_seen = http.trace.requests.lock().unwrap().len();
        // Preserve and re-await this exact pinned owner, never reconstruct a release.
        cancel.cancel();
        let joined = tokio::time::timeout(WAIT, &mut future).await;
        let result = match joined {
            Ok(r) => r.unwrap(),
            Err(_) => {
                let actual = future.await;
                panic!("cooperative join deadline after actual await: {actual:?}");
            }
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while http.trace.closed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled HTTP connection did not close");
        let trace = http.finish().await;
        assert!(timed_out);
        assert_eq!(request_seen, 1);
        assert!(!result.passed);
        assert_joined(&result);
        assert_eq!(
            b.markers("started-").len(),
            1,
            "queued cancelled attempt started another child"
        );
        assert_eq!(trace.requests.lock().unwrap().len(), 1);
        assert_eq!(trace.closed.load(Ordering::SeqCst), 1);
        assert_eq!(trace.joined.load(Ordering::SeqCst), 1);
        assert_no_audio(&b);
        assert_no_temporaries(&root);
        assert_cache_unlocked(&root);
        no_parent_tasks().await;
    }

    #[tokio::test]
    async fn cancellation_during_real_playback_stops_future_pcm_and_joins_before_return() {
        let b = Bundle::new();
        let bytes = [3, 2].repeat(16_000 * 3);
        let path = one(&b, std::slice::from_ref(&bytes), "playing", "normal");
        let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
        let plan = ReleaseManifest::load_user_speech(path, settings(&http.url, None)).unwrap();
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let mut task = tokio::spawn(async move { run(plan, c).await });
        let seen = tokio::time::timeout(Duration::from_secs(4), wait_for(&b, "audio-")).await;
        cancel.cancel();
        let result = (&mut task).await.unwrap().unwrap();
        let _ = http.finish().await;
        assert!(seen.is_ok());
        assert!(!result.passed);
        assert_joined(&result);
        let file = b.markers("audio-").remove(0);
        let before = std::fs::read(&file).unwrap();
        assert!(!before.is_empty() && before.len() < bytes.len());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            std::fs::read(file).unwrap(),
            before,
            "PCM changed after cooperative release return"
        );
        no_parent_tasks().await;
    }

    #[test]
    fn exact_16mib_actual_wav_reads_and_one_byte_over_are_distinct_from_pcm_quota() {
        let b = Bundle::new();
        let mut clip = wav(&[1, 2]);
        let payload = 1024 * 1024 - clip.len() - 8;
        clip.extend_from_slice(b"JUNK");
        clip.extend_from_slice(&(payload as u32).to_le_bytes());
        clip.resize(1024 * 1024, 0);
        let riff = (clip.len() - 8) as u32;
        clip[4..8].copy_from_slice(&riff.to_le_bytes());
        assert_eq!(
            pipecat_evals::RecordedAudio::from_wav(&clip)
                .unwrap()
                .pcm_bytes(),
            [1, 2]
        );
        let mut s = source("wav-read-bound", &[]);
        s["turns"] = json!((0..16)
            .map(|n| {
                let file = format!("metadata-{n}.wav");
                b.put(&file, &clip);
                json!({"user":"tiny PCM, large RIFF metadata","audio":file})
            })
            .collect::<Vec<_>>());
        b.json("wav.json", &s);
        let path = b.json(
            "manifest.json",
            &manifest(vec![entry(&b, "wav", "wav.json", "unused")], 1, 1),
        );
        let configured = settings("http://127.0.0.1:9", None);
        assert!(ReleaseManifest::load_user_speech(&path, configured.clone()).is_ok());
        // The extra byte is valid trailing file data outside RIFF; planned/read
        // file bytes, rather than the 32 PCM bytes, must reject before copying.
        clip.push(0);
        b.put("metadata-15.wav", clip);
        assert!(matches!(
            ReleaseManifest::load_user_speech(path, configured),
            Err(EvalError::Limit("recording bundle bytes"))
        ));
        assert!(b.markers("started-").is_empty());
    }

    #[tokio::test]
    async fn early_assertion_success_still_settles_local_playback_and_cache() {
        let b = Bundle::new();
        let root = b.cache();
        let bytes = pcm(187);
        let path = one(&b, std::slice::from_ref(&bytes), "early", "early");
        let mut s = source("early", &["exact fixture reference"]);
        s["turns"][0]["expect"][0]["text_contains"] = "early assertion".into();
        b.json("early.json", &s);
        let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
        let started = tokio::time::Instant::now();
        let result = run(
            ReleaseManifest::load_user_speech(path, settings(&http.url, Some(&root))).unwrap(),
            Cancellation::new(),
        )
        .await
        .unwrap();
        let elapsed = started.elapsed();
        let _ = http.finish().await;
        assert!(result.passed, "{result:?}");
        assert_joined(&result);
        assert!(elapsed >= Duration::from_millis(110));
        assert_eq!(cache_wavs(&root).len(), 1);
        assert_eq!(
            pipecat_evals::RecordedAudio::from_wav(&std::fs::read(&cache_wavs(&root)[0]).unwrap())
                .unwrap()
                .pcm_bytes(),
            bytes
        );
        assert_no_temporaries(&root);
        assert_cache_unlocked(&root);
        no_parent_tasks().await;
        // An early assertion proves local settlement only. The other real-route
        // tests separately require a response produced after full child consumption.
    }

    #[tokio::test]
    async fn cache_lock_timeout_and_post_startup_cancellation_return_with_settled_owners() {
        let prime_bundle = Bundle::new();
        let bytes = pcm(197);
        let (configured, root) = prime(&prime_bundle, &bytes).await;
        let lock_path = cache_wavs(&root)[0].parent().unwrap().join(".lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::LockExclusive).unwrap();
        for cancel_early in [false, true] {
            let b = Bundle::new();
            let path = one(&b, std::slice::from_ref(&bytes), "locked", "normal");
            let plan = ReleaseManifest::load_user_speech(path, configured.clone()).unwrap();
            let cancel = Cancellation::new();
            let c = cancel.clone();
            let mut task = tokio::spawn(async move { run(plan, c).await });
            let observed =
                tokio::time::timeout(Duration::from_secs(3), wait_for(&b, "started-")).await;
            if cancel_early {
                // Startup is not a cache-worker admission ACK. This branch
                // checks cancellation after child startup with the cache locked;
                // precise in-lookup cancellation belongs to the owner unit seam.
                tokio::time::sleep(Duration::from_millis(100)).await;
                cancel.cancel();
            }
            let result = (&mut task).await.unwrap().unwrap();
            assert!(observed.is_ok());
            assert!(!result.passed);
            assert_joined(&result);
            assert_no_audio(&b);
            assert!(
                failures(&result).contains(&if cancel_early {
                    "cancelled"
                } else {
                    "user_speech_cache_timeout"
                }),
                "{result:?}"
            );
            no_parent_tasks().await;
        }
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::Unlock).unwrap();
        assert_cache_unlocked(&root);
        assert_no_temporaries(&root);
    }

    #[tokio::test]
    async fn runtime_result_overflow_closes_admission_before_next_attempt() {
        let b = Bundle::new();
        let bytes = pcm(207);
        let path = one(&b, std::slice::from_ref(&bytes), "large", "large");
        let mut spec: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        spec["repeat"] = 2.into();
        b.json("manifest.json", &spec);
        let http = Http::start(vec![Reply::pcm(bytes)]).await;
        let plan = ReleaseManifest::load_user_speech(path, settings(&http.url, None)).unwrap();
        let cancel = Cancellation::new();
        let future =
            run_release_with_budget(plan, options(), process_options(), cancel.clone(), 4096);
        tokio::pin!(future);
        let result = match tokio::time::timeout(WAIT, &mut future).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                cancel.cancel();
                let actual = future.await;
                panic!("overflow drain deadline: {actual:?}");
            }
        };
        let trace = http.finish().await;
        assert!(!result.passed);
        assert!(failures(&result).contains(&"limit_exceeded"));
        assert_eq!(b.markers("started-").len(), 1);
        assert_eq!(trace.requests.lock().unwrap().len(), 1);
        // Full results intentionally disappear on overflow. Observe no remaining
        // child and await runtime task quiescence without inventing a reap report.
        for marker in b.markers("started-") {
            let pid = std::fs::read_to_string(marker).unwrap();
            #[cfg(target_os = "linux")]
            assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        }
        no_parent_tasks().await;
    }
}

#[cfg(all(feature = "user-speech", not(feature = "bot-audio-transcription")))]
#[tokio::test]
async fn missing_transcription_feature_precedes_both_invalid_joint_runtime_files() {
    let b = Bundle::new();
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache = b.cache();
    b.json("one.json", &source("one", &["reference"]));
    let manifest = b.json(
        "manifest.json",
        &manifest(vec![entry(&b, "one", "one.json", "unused")], 1, 1),
    );
    let speech = b.put("speech.json", b"invalid speech JSON must not be parsed");
    let stt = b.put("stt.json", b"invalid transcription JSON must not be parsed");
    let mut args = arguments(&manifest, &speech);
    args.extend(["--bot-audio-response".into(), stt.display().to_string()]);
    let output = cli(&args, &b.0).await;
    assert_exit(&output, 2);
    assert!(String::from_utf8_lossy(&output.stderr).contains("bot-audio-transcription"));
    assert!(output.stdout.is_empty());
    assert!(b.markers("started-").is_empty());
    assert_eq!(std::fs::read_dir(cache).unwrap().count(), 0);
    assert_idle(&provider).await;
}

#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
#[path = "support/hosted_audio_to_audio.rs"]
mod duplex_fixture;

#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
#[tokio::test]
async fn audio_to_audio_suite_bot() {
    duplex_fixture::child().await;
}

#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
mod joint {
    use super::{
        duplex_fixture as duplex,
        fixture::{self, enabled as input, Bundle},
    };
    use duplex::{Http, Kind, Reply, LLM, REFERENCE, SPOKEN};
    use pipecat_evals::{
        release::{run_release_with_budget, ReleaseManifest, ReleaseResult},
        BotTranscriptionSettings, Cancellation, CartesiaUserSpeechSettings, EvalError,
        SessionOptions,
    };
    use serde_json::{json, Value};
    use std::{
        path::{Path, PathBuf},
        sync::atomic::Ordering,
        time::Duration,
    };

    async fn run_path(
        path: &Path,
        speech: CartesiaUserSpeechSettings,
        stt: BotTranscriptionSettings,
        options: SessionOptions,
        cancel: Cancellation,
    ) -> Result<ReleaseResult, EvalError> {
        duplex::run(
            ReleaseManifest::load_audio_to_audio(path, speech, stt)?,
            options,
            cancel,
        )
        .await
    }
    fn value(result: &ReleaseResult) -> Value {
        serde_json::to_value(result).unwrap()
    }
    fn failures(result: &ReleaseResult) -> Vec<&str> {
        result
            .runs
            .iter()
            .flat_map(|run| {
                run.hosted
                    .result
                    .failures
                    .iter()
                    .map(|failure| failure.kind.as_str())
            })
            .collect()
    }
    fn runtime_files(b: &Bundle, tts: &str, stt: &str, cache: Option<&Path>) -> (PathBuf, PathBuf) {
        let mut runtime = fixture::runtime(tts, cache);
        runtime["request_timeout_ms"] = 10000.into();
        (
            b.json("joint-speech-runtime.json", &runtime),
            b.json("joint-stt-runtime.json", &duplex::stt_runtime(stt)),
        )
    }
    fn release_tail_gates(b: &Bundle) {
        for marker in b.markers("started-") {
            let name = marker.file_name().unwrap().to_string_lossy();
            b.put(&name.replace("started-", "release-"), b"go");
        }
    }

    #[tokio::test]
    async fn both_cli_flag_orders_run_joint_attempt_major_complete_pcm_and_spoken_send_after() {
        for reverse in [false, true] {
            let b = Bundle::new();
            let speech = input::pcm(21);
            let recording = input::pcm(41);
            b.put("override.wav", input::wav(&recording));
            let mut entries = vec![];
            for (name, wait_delta) in [("slow", 1), ("fast", 0)] {
                let mut scenario = duplex::source(name);
                scenario["turns"] = json!([
                    {"user":REFERENCE,"expect":[duplex::expectation("llm_response",LLM)]},
                    {"user":"WAV reference never synthesized","audio":"override.wav","send_after":{"event":"response"},"expect":[duplex::expectation("response",SPOKEN)]}
                ]);
                b.json(&format!("{name}.json"), &scenario);
                input::profile(
                    &b,
                    name,
                    &[speech.clone(), recording.clone()],
                    &format!("isolated {name}"),
                    "normal",
                    wait_delta,
                );
                entries.push(duplex::entry(&b, name, &format!("{name}.json"), name));
            }
            let spec = fixture::manifest(entries, 2, 2);
            let base = spec["base_port"].as_u64().unwrap();
            let manifest = b.json("manifest.json", &spec);
            let tts = Http::start(Kind::Tts, vec![Reply::Pcm(speech.clone()); 4]).await;
            let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN); 8]).await;
            let (speech_runtime, stt_runtime) = runtime_files(&b, &tts.url, &stt.url, None);
            let output = duplex::cli(
                &duplex::arguments(&manifest, &speech_runtime, &stt_runtime, reverse),
                &b.0,
            )
            .await;
            let tts = tts.finish().await;
            let stt = stt.finish().await;
            let result = duplex::result(&output, 0);
            tts.checked(4);
            stt.checked(8);
            assert_eq!(result["passed"], true);
            let runs = result["runs"].as_array().unwrap();
            assert_eq!(
                runs.iter()
                    .map(|r| (r["attempt"].as_u64().unwrap(), r["name"].as_str().unwrap()))
                    .collect::<Vec<_>>(),
                vec![(1, "slow"), (1, "fast"), (2, "slow"), (2, "fast")]
            );
            assert!(runs
                .iter()
                .all(|run| duplex::responses(run) == [SPOKEN, SPOKEN]));
            input::assert_consumed(
                &b,
                &result,
                &vec![vec![speech.clone(), recording.clone()]; 4],
            );
            assert!(tts.gates.iter().all(|gate| duplex::request_json(
                &gate.request.lock().unwrap()
            )["transcript"]
                == REFERENCE));
            let order = std::fs::read_to_string(b.0.join("completion-order"))
                .unwrap()
                .lines()
                .map(|line| line.parse::<u64>().unwrap())
                .collect::<Vec<_>>();
            for offset in [0, 2] {
                assert!(
                    order.iter().position(|port| *port == base + offset + 1)
                        < order.iter().position(|port| *port == base + offset)
                );
            }
            assert_eq!(b.markers("started-").len(), 4);
            assert!(b.markers("cache").is_empty());
        }
    }

    #[tokio::test]
    async fn joint_cli_cannot_succeed_before_child_consumes_complete_user_pcm() {
        let b = Bundle::new();
        let bytes = input::pcm(61);
        let path = duplex::one(&b, "tail", std::slice::from_ref(&bytes), "gated");
        let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN)]).await;
        let (speech, transcription) = runtime_files(&b, &tts.url, &stt.url, None);
        let args = duplex::arguments(&path, &speech, &transcription, false);
        let cwd = b.0.clone();
        let task = tokio::spawn(async move { duplex::cli(&args, &cwd).await });
        let tail = duplex::observed(|| !b.markers("tail-1-").is_empty()).await;
        let prematurely_finished = task.is_finished();
        let consumed = b.markers("consumed-").len();
        let stt_before_complete = stt.gates[0].request.lock().unwrap().len();
        release_tail_gates(&b);
        let output = task.await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        assert!(tail);
        assert!(!prematurely_finished);
        assert_eq!(consumed, 0);
        assert_eq!(stt_before_complete, 0);
        let result = duplex::result(&output.unwrap(), 0);
        tts.checked(1);
        stt.checked(1);
        input::assert_consumed(&b, &result, &[vec![bytes]]);
    }

    #[tokio::test]
    async fn joint_cold_warm_cache_and_wav_override_preserve_per_child_pcm() {
        let b = Bundle::new();
        let cache = b.cache();
        let speech = input::pcm(81);
        let recorded = input::pcm(101);
        b.put("override.wav", input::wav(&recorded));
        let mut entries = vec![];
        for name in ["first", "second"] {
            let mut scenario = duplex::source(name);
            scenario["turns"].as_array_mut().unwrap().push(json!({"user":"WAV reference must stay context","audio":"override.wav","expect":[duplex::expectation("response",SPOKEN)]}));
            b.json(&format!("{name}.json"), &scenario);
            input::profile(
                &b,
                name,
                &[speech.clone(), recorded.clone()],
                &format!("isolated {name}"),
                "normal",
                0,
            );
            entries.push(duplex::entry(&b, name, &format!("{name}.json"), name));
        }
        let mut spec = fixture::manifest(entries, 1, 2);
        let path = b.json("manifest.json", &spec);
        let tts = Http::start(Kind::Tts, vec![Reply::Pcm(speech.clone())]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN); 8]).await;
        let (speech_runtime, stt_runtime) = runtime_files(&b, &tts.url, &stt.url, Some(&cache));
        let cold = duplex::cli(
            &duplex::arguments(&path, &speech_runtime, &stt_runtime, false),
            &b.0,
        )
        .await;
        let tts = tts.finish().await;
        // The warm CLI retains a closed TTS URL: successful input must use the cache.
        spec["base_port"] = fixture::manifest(vec![], 1, 1)["base_port"].clone();
        b.json("manifest.json", &spec);
        let warm = duplex::cli(
            &duplex::arguments(&path, &speech_runtime, &stt_runtime, true),
            &b.0,
        )
        .await;
        let stt = stt.finish().await;
        tts.checked(1);
        stt.checked(8);
        for output in [cold, warm] {
            let result = duplex::result(&output, 0);
            input::assert_consumed(
                &b,
                &result,
                &vec![vec![speech.clone(), recorded.clone()]; 2],
            );
        }
        let files = input::cache_wavs(&cache);
        assert_eq!(files.len(), 1);
        assert_eq!(
            pipecat_evals::RecordedAudio::from_wav(&std::fs::read(&files[0]).unwrap())
                .unwrap()
                .pcm_bytes(),
            speech
        );
        input::assert_no_temporaries(&cache);
        input::assert_cache_unlocked(&cache);
    }

    #[tokio::test]
    async fn joint_all_wav_with_unused_voice_never_creates_tts_or_cache_work() {
        let b = Bundle::new();
        let cache = b.cache();
        let bytes = input::pcm(121);
        let path = duplex::one(&b, "all-wav", std::slice::from_ref(&bytes), "normal");
        b.put("only.wav", input::wav(&bytes));
        let mut scenario = duplex::source("all-wav");
        // Keep the valid inherited voice to exercise the actual all-WAV joint branch.
        scenario["turns"][0]["audio"] = "only.wav".into();
        b.json("all-wav.json", &scenario);
        let tts = Http::start(Kind::Tts, vec![]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN)]).await;
        let result = run_path(
            &path,
            input::settings(&tts.url, Some(&cache)),
            duplex::stt_settings(&b, &stt.url),
            duplex::options(),
            Cancellation::new(),
        )
        .await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        let retired = duplex::retired().await;
        let result = result.unwrap();
        assert!(result.passed, "{result:?}");
        tts.checked(0);
        stt.checked(1);
        input::assert_consumed(&b, &value(&result), &[vec![bytes]]);
        assert_eq!(std::fs::read_dir(cache).unwrap().count(), 0);
        assert!(retired);
    }

    #[tokio::test]
    async fn joint_judge_receives_reference_and_spoken_response_while_exact_uses_no_judge() {
        for semantic in [false, true] {
            let b = Bundle::new();
            let bytes = input::pcm(141);
            let path = duplex::one(&b, "judge", std::slice::from_ref(&bytes), "normal");
            let judge = Http::start(
                Kind::Judge,
                if semantic { vec![Reply::Judge] } else { vec![] },
            )
            .await;
            let mut scenario = duplex::source("judge");
            scenario["judge"] = duplex::judge_settings(&judge.url);
            if semantic {
                scenario["turns"][0]["expect"] = json!([{"event":"response","eval":"satisfies the spoken criterion","within_ms":7000}]);
            }
            b.json("judge.json", &scenario);
            let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
            let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN)]).await;
            let (speech_runtime, stt_runtime) = runtime_files(&b, &tts.url, &stt.url, None);
            let output = duplex::cli(
                &duplex::arguments(&path, &speech_runtime, &stt_runtime, false),
                &b.0,
            )
            .await;
            let tts = tts.finish().await;
            let stt = stt.finish().await;
            let judge = judge.finish().await;
            tts.checked(1);
            stt.checked(1);
            judge.checked(usize::from(semantic));
            let result = duplex::result(&output, 0);
            input::assert_consumed(&b, &result, &[vec![bytes]]);
            if semantic {
                let body = duplex::request_json(&judge.gates[0].request.lock().unwrap());
                assert_eq!(
                    body["state"],
                    json!({"conversation":[{"speaker":"user","text":REFERENCE}],"latest_bot_reply":SPOKEN})
                );
                assert!(!body.to_string().contains(LLM));
                assert!(!body.to_string().contains(duplex::TTS));
                assert_eq!(
                    result["runs"][0]["hosted"]["result"]["turns"][0]["expectations"][0]
                        ["judgments"][0]["decision"],
                    "yes"
                );
            }
        }
    }

    #[tokio::test]
    async fn joint_matching_reference_llm_and_tts_decoys_cannot_replace_bot_pcm() {
        for decoy in [LLM, duplex::TTS] {
            let b = Bundle::new();
            let bytes = input::pcm(161);
            let path = duplex::one(&b, "silent", std::slice::from_ref(&bytes), "silent");
            let mut scenario = duplex::source("silent");
            scenario["turns"][0]["user"] = decoy.into();
            scenario["turns"][0]["expect"] =
                json!([{"event":"response","text_contains":decoy,"within_ms":500}]);
            b.json("silent.json", &scenario);
            let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
            let stt = Http::start(Kind::Stt, vec![]).await;
            let result = run_path(
                &path,
                input::settings(&tts.url, None),
                duplex::stt_settings(&b, &stt.url),
                duplex::options(),
                Cancellation::new(),
            )
            .await;
            let tts = tts.finish().await;
            let stt = stt.finish().await;
            tts.checked(1);
            stt.checked(0);
            let result = result.unwrap();
            assert!(!result.passed);
            assert!(duplex::responses(&value(&result)["runs"][0]).is_empty());
            input::assert_consumed(&b, &value(&result), &[vec![bytes]]);
        }
    }

    #[tokio::test]
    async fn joint_final_llm_success_still_waits_for_admitted_stt_prefix() {
        let b = Bundle::new();
        let bytes = input::pcm(181);
        let path = duplex::one(&b, "settle", std::slice::from_ref(&bytes), "normal");
        let mut scenario = duplex::source("settle");
        scenario["turns"][0]["expect"] = json!([duplex::expectation("llm_response", LLM)]);
        b.json("settle.json", &scenario);
        let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Gate(SPOKEN)]).await;
        let speech = input::settings(&tts.url, None);
        let transcription = duplex::stt_settings(&b, &stt.url);
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            run_path(&path, speech, transcription, duplex::options(), c).await
        });
        let entered = duplex::observed(|| !stt.gates[0].request.lock().unwrap().is_empty()).await;
        let emitted = duplex::observed(|| !b.markers("emitted-").is_empty()).await;
        let finished_before_stt = task.is_finished();
        stt.gates[0].released.add_permits(1);
        if !entered || !emitted {
            cancel.cancel();
        }
        let result = task.await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        assert!(entered && emitted);
        assert!(
            !finished_before_stt,
            "reported LLM success skipped held STT settlement"
        );
        tts.checked(1);
        stt.checked(1);
        let result = result.unwrap().unwrap();
        assert!(result.passed, "{result:?}");
        assert_eq!(duplex::responses(&value(&result)["runs"][0]), [SPOKEN]);
        input::assert_consumed(&b, &value(&result), &[vec![bytes]]);
    }

    #[tokio::test]
    async fn joint_fixed_final_settlement_timeout_closes_stt_before_fixture_cleanup() {
        let b = Bundle::new();
        let bytes = input::pcm(201);
        let path = duplex::one(&b, "timeout", std::slice::from_ref(&bytes), "normal");
        let mut scenario = duplex::source("timeout");
        scenario["turns"][0]["expect"] = json!([duplex::expectation("llm_response", LLM)]);
        b.json("timeout.json", &scenario);
        let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Hold]).await;
        let mut options = duplex::options();
        options.bot_quiet_timeout = Duration::from_millis(2500);
        let result = run_path(
            &path,
            input::settings(&tts.url, None),
            duplex::stt_settings(&b, &stt.url),
            options,
            Cancellation::new(),
        )
        .await;
        let eof = duplex::observed(|| stt.gates[0].closed.load(Ordering::SeqCst)).await;
        let reaped_before_cleanup = duplex::child_pids_gone(&b);
        let pipes_before_cleanup = duplex::retired_except_fixtures(2).await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        let retired = duplex::retired().await;
        assert!(eof && reaped_before_cleanup && pipes_before_cleanup && retired);
        tts.checked(1);
        stt.checked(1);
        let result = result.unwrap();
        assert!(!result.passed);
        assert!(
            failures(&result).contains(&"bot_transcription_timeout"),
            "{result:?}"
        );
        input::assert_reaped(&value(&result));
    }

    #[tokio::test]
    async fn joint_borrowed_owner_cancel_closes_held_tts_and_stt_before_fixture_cleanup() {
        for hold_tts in [true, false] {
            let b = Bundle::new();
            let cache = b.cache();
            let bytes = input::pcm(221);
            let path = duplex::one(&b, "cancel", std::slice::from_ref(&bytes), "normal");
            let mut spec: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            spec["repeat"] = 2.into();
            b.json("cancel-manifest.json", &spec);
            let tts = Http::start(
                Kind::Tts,
                vec![if hold_tts {
                    Reply::Hold
                } else {
                    Reply::Pcm(bytes.clone())
                }],
            )
            .await;
            let stt =
                Http::start(Kind::Stt, if hold_tts { vec![] } else { vec![Reply::Hold] }).await;
            let mut speech = pipecat_speech::SpeechConfig::cartesia(fixture::SECRET);
            speech.base_url = tts.url.clone();
            speech.timeout = Duration::from_secs(10);
            let speech = CartesiaUserSpeechSettings::new(
                speech,
                pipecat_evals::UserSpeechCachePolicy::Private {
                    directory: cache.clone(),
                    max_entries: 8,
                    max_bytes: 16 * 1024 * 1024,
                },
            )
            .unwrap();
            let transcription = duplex::stt_settings(&b, &stt.url);
            let cancel = Cancellation::new();
            let future = run_path(
                &path,
                speech,
                transcription,
                duplex::options(),
                cancel.clone(),
            );
            tokio::pin!(future);
            let gate = if hold_tts {
                tts.gates[0].clone()
            } else {
                stt.gates[0].clone()
            };
            // Poll the exact pinned release alongside a real provider-entry barrier.
            let entry =
                tokio::select! {result=&mut future=>Err(result),_=gate.received.notified()=>Ok(())};
            let mut premature = None;
            let (entered, timed_out) = match entry {
                Ok(()) => {
                    match tokio::time::timeout(Duration::from_millis(25), &mut future).await {
                        Err(_) => (true, true),
                        Ok(result) => {
                            premature = Some(result);
                            (true, false)
                        }
                    }
                }
                Err(result) => {
                    premature = Some(result);
                    (false, false)
                }
            };
            cancel.cancel();
            let result = match premature {
                Some(result) => result,
                None => future.await,
            };
            let eof = duplex::observed(|| gate.closed.load(Ordering::SeqCst)).await;
            let reaped_before_cleanup = duplex::child_pids_gone(&b);
            let pipes_before_cleanup = duplex::retired_except_fixtures(2).await;
            let tts = tts.finish().await;
            let stt = stt.finish().await;
            let retired = duplex::retired().await;
            assert!(
                entered
                    && timed_out
                    && eof
                    && reaped_before_cleanup
                    && pipes_before_cleanup
                    && retired,
                "hold_tts={hold_tts} entered={entered} timed_out={timed_out} eof={eof} reaped={reaped_before_cleanup} pipes={pipes_before_cleanup} retired={retired} result={result:?}"
            );
            tts.checked(1);
            stt.checked(usize::from(!hold_tts));
            let result = result.unwrap();
            assert!(!result.passed);
            input::assert_reaped(&value(&result));
            assert_eq!(b.markers("started-").len(), 1);
            if hold_tts {
                input::assert_no_audio(&b);
                assert!(input::cache_wavs(&cache).is_empty());
            }
            input::assert_no_temporaries(&cache);
            input::assert_cache_unlocked(&cache);
        }
    }

    #[tokio::test]
    async fn joint_concurrent_retention_overflow_drains_active_stt_and_keeps_queued_child_unstarted(
    ) {
        let b = Bundle::new();
        let bytes = input::pcm(241);
        let mut entries = vec![];
        for (name, mode) in [
            ("large", "large"),
            ("active", "normal"),
            ("queued", "normal"),
        ] {
            input::profile(
                &b,
                name,
                std::slice::from_ref(&bytes),
                &format!("isolated {name}"),
                mode,
                0,
            );
            let mut scenario = duplex::source(name);
            if name == "large" {
                scenario["turns"][0]["user"] = "large result waits for active STT".into();
            }
            b.json(&format!("{name}.json"), &scenario);
            entries.push(duplex::entry(&b, name, &format!("{name}.json"), name));
        }
        let path = b.json("manifest.json", &fixture::manifest(entries, 1, 2));
        let tts = Http::start(
            Kind::Tts,
            vec![
                Reply::PcmAfterMarker {
                    bytes: bytes.clone(),
                    transcript: "large result waits for active STT",
                    marker: b.0.join("stt-held"),
                };
                2
            ],
        )
        .await;
        // Hold large input at HTTP, before playback. Blocking the child's input
        // actor here would fill its lane with synthetic silence before STT entry.
        // Active's inspected STT request must be held before large input arrives.
        let stt = Http::with_marker(
            Kind::Stt,
            vec![Reply::Hold, Reply::Text(SPOKEN)],
            Some(b.0.join("stt-held")),
        )
        .await;
        let mut speech = pipecat_speech::SpeechConfig::cartesia(fixture::SECRET);
        speech.base_url = tts.url.clone();
        // This HTTP request intentionally waits for the other attempt's PCM/VAD;
        // the ordinary 600ms fixture TTS deadline is not a valid causal hold.
        speech.timeout = Duration::from_secs(10);
        let speech =
            CartesiaUserSpeechSettings::new(speech, pipecat_evals::UserSpeechCachePolicy::Disabled)
                .unwrap();
        let plan =
            ReleaseManifest::load_audio_to_audio(&path, speech, duplex::stt_settings(&b, &stt.url));
        let cancel = Cancellation::new();
        let result = match plan {
            Ok(plan) => {
                let future = run_release_with_budget(
                    plan,
                    duplex::options(),
                    duplex::process_options(),
                    cancel.clone(),
                    8192,
                );
                tokio::pin!(future);
                match tokio::time::timeout(duplex::WAIT * 2, &mut future).await {
                    Ok(result) => result,
                    Err(_) => {
                        cancel.cancel();
                        let _actual = future.await;
                        Err(EvalError::Timeout("joint overflow fixture deadline"))
                    }
                }
            }
            Err(error) => Err(error),
        };
        let eof = duplex::observed(|| stt.gates[0].closed.load(Ordering::SeqCst)).await;
        let gone_before_cleanup = duplex::child_pids_gone(&b);
        let pipes_before_cleanup = duplex::retired_except_fixtures(2).await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        let retired = duplex::retired().await;
        assert!(eof && gone_before_cleanup && pipes_before_cleanup && retired,
            "eof={eof} gone={gone_before_cleanup} pipes={pipes_before_cleanup} retired={retired} result={result:?}");
        assert_eq!(b.markers("started-").len(), 2, "result={result:?}");
        assert!(
            tts.problems.is_empty(),
            "TTS {:?}; result={result:?}",
            tts.problems
        );
        assert!(
            stt.problems.is_empty(),
            "STT {:?}; result={result:?}",
            stt.problems
        );
        tts.checked(2);
        stt.checked(2);
        assert_eq!(b.markers("started-").len(), 2);
        let result = result.unwrap();
        assert!(!result.passed);
        assert!(failures(&result).contains(&"limit_exceeded"));
        // Overflow intentionally retains summaries. /proc and runtime quiescence
        // independently establish child and pipe retirement; no invented reap report.
        // Queued zero-model evidence belongs to the private loader-counter test.
    }

    fn precharged(
        b: &Bundle,
        name: &str,
        bytes: &[u8],
        wav_bytes: usize,
        repeat: usize,
        admitted: bool,
    ) -> PathBuf {
        let mut scenario = duplex::source(name);
        if admitted {
            scenario["turns"][0]["expect"] = json!([{"event":"response","text_contains":"intentionally unmatched stops before prepaid filler","within_ms":3000}]);
        }
        let mut remaining = wav_bytes;
        let mut n = 0;
        while remaining != 0 {
            let size = remaining.min(1_920_000);
            let file = format!("{name}-future-{n}.wav");
            b.put(&file, input::wav(&[9, 1].repeat(size / 2)));
            scenario["turns"].as_array_mut().unwrap().push(json!({"user":"future WAV conservatively prepaid","audio":file,"expect":[duplex::expectation("response","never played filler")]}));
            remaining -= size;
            n += 1;
        }
        b.json(&format!("{name}.json"), &scenario);
        input::profile(
            b,
            name,
            &[bytes.to_vec()],
            &format!("isolated {name}"),
            "normal",
            0,
        );
        b.json(
            &format!("{name}-manifest.json"),
            &fixture::manifest(
                vec![duplex::entry(b, name, &format!("{name}.json"), name)],
                repeat,
                1,
            ),
        )
    }

    #[tokio::test]
    async fn joint_shared_cache_hit_budget_exact_boundary_repeat_and_independent_plans() {
        let prime = Bundle::new();
        let cache = prime.cache();
        let bytes = input::pcm(151);
        let path = duplex::one(&prime, "prime", std::slice::from_ref(&bytes), "normal");
        let tts = Http::start(Kind::Tts, vec![Reply::Pcm(bytes.clone())]).await;
        let stt = Http::start(Kind::Stt, vec![Reply::Text(SPOKEN)]).await;
        let configured = input::settings(&tts.url, Some(&cache));
        let result = run_path(
            &path,
            configured.clone(),
            duplex::stt_settings(&prime, &stt.url),
            duplex::options(),
            Cancellation::new(),
        )
        .await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        tts.checked(1);
        stt.checked(1);
        let result = result.unwrap();
        assert!(result.passed, "{result:?}");
        input::assert_consumed(&prime, &value(&result), &[vec![bytes.clone()]]);
        let cached = std::fs::read(&input::cache_wavs(&cache)[0]).unwrap();
        for mode in ["exact", "repeat", "combined"] {
            let b = Bundle::new();
            let admitted = mode == "exact";
            let prepaid = input::PCM_BUDGET - bytes.len() + if admitted { 0 } else { 2 };
            let stt = Http::start(
                Kind::Stt,
                if admitted {
                    vec![Reply::Text(SPOKEN)]
                } else {
                    vec![]
                },
            )
            .await;
            let transcriber = duplex::stt_settings(&b, &stt.url);
            let plan = if mode == "combined" {
                let first = ReleaseManifest::load_audio_to_audio(
                    precharged(&b, "first", &bytes, prepaid / 2, 1, false),
                    configured.clone(),
                    duplex::stt_settings(&b, &stt.url),
                );
                let second = ReleaseManifest::load_audio_to_audio(
                    precharged(&b, "second", &bytes, prepaid / 2, 1, false),
                    configured.clone(),
                    transcriber,
                );
                match (first, second) {
                    (Ok(mut first), Ok(second)) => {
                        first.jobs.extend(second.jobs);
                        Ok(first)
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            } else {
                let repeat = if admitted { 1 } else { 2 };
                ReleaseManifest::load_audio_to_audio(
                    precharged(&b, mode, &bytes, prepaid / repeat, repeat, admitted),
                    configured.clone(),
                    transcriber,
                )
            };
            let result = match plan {
                Ok(plan) => duplex::run(plan, duplex::options(), Cancellation::new()).await,
                Err(error) => Err(error),
            };
            let stt = stt.finish().await;
            let retired = duplex::retired().await;
            stt.checked(usize::from(admitted));
            let result = result.unwrap();
            assert!(!result.passed);
            input::assert_reaped(&value(&result));
            assert!(retired);
            if admitted {
                input::assert_consumed(&b, &value(&result), &[vec![bytes.clone()]]);
                assert!(
                    !failures(&result).contains(&"limit_exceeded"),
                    "exact logical boundary rejected: {result:?}"
                );
            } else {
                input::assert_no_audio(&b);
                assert_eq!(b.markers("started-").len(), 2);
                assert!(
                    result.runs.iter().all(|run| run
                        .hosted
                        .result
                        .failures
                        .iter()
                        .any(|failure| failure.kind == "limit_exceeded"
                            && failure.reason.contains("PCM budget"))),
                    "{mode}: {result:?}"
                );
            }
            assert_eq!(
                std::fs::read(&input::cache_wavs(&cache)[0]).unwrap(),
                cached
            );
            input::assert_no_temporaries(&cache);
            input::assert_cache_unlocked(&cache);
        }
    }

    #[tokio::test]
    async fn joint_bad_last_job_public_mutation_options_and_summary_floor_start_no_child_http_or_cache(
    ) {
        let b = Bundle::new();
        let cache = b.cache();
        let tts = Http::start(Kind::Tts, vec![]).await;
        let stt = Http::start(Kind::Stt, vec![]).await;
        b.json("good.json", &duplex::source("good"));
        let path = b.json(
            "manifest.json",
            &fixture::manifest(vec![duplex::entry(&b, "good", "good.json", "unused")], 1, 1),
        );
        let configured = input::settings(&tts.url, Some(&cache));
        let mut errors = vec![];
        for mutation in ["last", "public", "expanded", "judge", "summary"] {
            let loaded = ReleaseManifest::load_audio_to_audio(
                &path,
                configured.clone(),
                duplex::stt_settings(&b, &stt.url),
            );
            let result = match loaded {
                Ok(mut plan) => {
                    let mut options = duplex::options();
                    match mutation {
                        "last" => {
                            let mut last = plan.jobs[0].clone();
                            last.name = "bad-later-job".into();
                            last.port += 1;
                            last.scenario.turns[0].user = Some("mutated later input".into());
                            plan.jobs.push(last);
                            plan.concurrency = 2;
                        }
                        "public" => {
                            plan.jobs[0].scenario.turns[0].expect[1].text_contains =
                                Some("mutated spoken expectation".into())
                        }
                        "expanded" => {
                            options.max_wire_messages =
                                SessionOptions::default().max_wire_messages + 1
                        }
                        "judge" => {
                            plan.jobs[0].scenario.turns[0].expect[1].eval =
                                Some("missing judge must fail".into())
                        }
                        "summary" => {
                            for attempt in 2..=4 {
                                let mut cloned = plan.jobs[0].clone();
                                cloned.attempt = attempt;
                                cloned.port += (attempt - 1) as u16;
                                plan.jobs.push(cloned);
                            }
                        }
                        _ => {}
                    }
                    if mutation == "summary" {
                        run_release_with_budget(
                            plan,
                            options,
                            duplex::process_options(),
                            Cancellation::new(),
                            1024,
                        )
                        .await
                    } else {
                        duplex::run(plan, options, Cancellation::new()).await
                    }
                }
                Err(error) => Err(error),
            };
            errors.push((mutation, result.err()));
        }
        let mut bad = duplex::source("bad");
        bad["turns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"dtmf":"1"}));
        b.json("bad.json", &bad);
        let invalid = b.json(
            "invalid.json",
            &fixture::manifest(
                vec![
                    duplex::entry(&b, "good", "good.json", "unused"),
                    duplex::entry(&b, "bad", "bad.json", "unused"),
                ],
                1,
                2,
            ),
        );
        let (speech_runtime, stt_runtime) = runtime_files(&b, &tts.url, &stt.url, Some(&cache));
        let invalid_cli = duplex::cli(
            &duplex::arguments(&invalid, &speech_runtime, &stt_runtime, false),
            &b.0,
        )
        .await;
        let mut missing_judge = duplex::source("missing-judge");
        missing_judge["turns"][0]["expect"] =
            json!([{"event":"response","eval":"must have an explicit judge","within_ms":7000}]);
        b.json("missing-judge.json", &missing_judge);
        let judge_path = b.json(
            "missing-judge-manifest.json",
            &fixture::manifest(
                vec![duplex::entry(
                    &b,
                    "missing-judge",
                    "missing-judge.json",
                    "unused",
                )],
                1,
                1,
            ),
        );
        let judge_cli = duplex::cli(
            &duplex::arguments(&judge_path, &speech_runtime, &stt_runtime, false),
            &b.0,
        )
        .await;
        let mut smart = duplex::stt_runtime(&stt.url);
        // Reuse the existing authorized local file only for static asset validation.
        // Joint validation must reject Smart Turn before any model is loaded.
        smart["smart_turn_model"] = smart["silero_model"].clone();
        b.json("joint-stt-runtime.json", &smart);
        let smart_cli = duplex::cli(
            &duplex::arguments(&path, &speech_runtime, &stt_runtime, true),
            &b.0,
        )
        .await;
        let tts = tts.finish().await;
        let stt = stt.finish().await;
        tts.checked(0);
        stt.checked(0);
        assert!(
            errors.iter().all(|(_, error)| error.is_some()),
            "{errors:?}"
        );
        let invalid_cli = invalid_cli.unwrap();
        let smart_cli = smart_cli.unwrap();
        let judge_cli = judge_cli.unwrap();
        assert!(String::from_utf8_lossy(&smart_cli.stderr).contains("Smart Turn"));
        assert!(String::from_utf8_lossy(&judge_cli.stderr).contains("judge"));
        for output in [invalid_cli, smart_cli, judge_cli] {
            fixture::assert_exit(&output, 2);
            assert!(output.stdout.is_empty());
        }
        assert!(b.markers("started-").is_empty());
        assert_eq!(std::fs::read_dir(cache).unwrap().count(), 0);
        // These observations prove zero child/HTTP/cache activity, not zero model loads.
        // The private lib tests independently instrument model preflight and attempts.
    }
}
