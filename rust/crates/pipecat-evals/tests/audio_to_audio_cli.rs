//! Actual CLI processes and actual native host / loopback provider owners.
//! Every child is reaped and every fixture owner joined before assertions.
#![cfg(unix)]

#[allow(dead_code)]
#[path = "support/user_speech_common.rs"]
mod common;
#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
#[allow(dead_code)]
#[path = "support/audio_to_audio_fixture.rs"]
mod fixture;
#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
#[allow(dead_code)]
#[path = "support/user_speech.rs"]
mod input_fixture;
use common::{Bundle, Error, SECRET};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpListener, process::Command, sync::Notify};

const CLI_WAIT: Duration = Duration::from_secs(20);
const TTS_ENV: &str = "PIPECAT_DUPLEX_FIXTURE_TTS_KEY_125";
const STT_ENV: &str = "PIPECAT_DUPLEX_FIXTURE_STT_KEY_125";
const STT_KEY: &str = "synthetic-duplex-stt-key-125";
#[allow(dead_code)]
enum CancelAt<'a> {
    Never,
    Http(&'a Notify),
}
async fn output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = vec![];
    (&mut pipe)
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 256 * 1024 {
        return Err(std::io::Error::other("CLI output budget"));
    }
    Ok(bytes)
}
fn interrupt(pid: Option<u32>) -> Result<(), Error> {
    let pid = pid.ok_or("CLI child has no live PID")?;
    let status = std::process::Command::new("kill")
        .args(["-INT", &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err("could not signal fixture CLI child".into());
    }
    Ok(())
}
async fn cli(args: &[String], cwd: &Path, cancel: CancelAt<'_>) -> Result<Output, Error> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut child = Command::new(executable)
        .args(args)
        .current_dir(cwd)
        .env(TTS_ENV, SECRET)
        .env(STT_ENV, STT_KEY)
        .env("CARTESIA_API_KEY", "unapproved-fallback-sentinel")
        .env("OPENAI_API_KEY", "unapproved-fallback-sentinel")
        .env("PIPECAT_ONNX_RUNTIME", "/missing/ambient-runtime.so")
        .env("PIPECAT_SILERO_MODEL", "/missing/ambient-silero.onnx")
        // Explicit endpoint files contain literal loopback. Keep proxy behavior
        // deterministic without making any claim that STT bypasses all proxies.
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("https_proxy", "http://127.0.0.1:9")
        .env("all_proxy", "http://127.0.0.1:9")
        .env("NO_PROXY", "127.0.0.1")
        .env("no_proxy", "127.0.0.1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = tokio::spawn(output(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(output(child.stderr.take().unwrap()));
    let mut problems = vec![];
    let reached = match cancel {
        CancelAt::Never => None,
        CancelAt::Http(ready) => Some(
            tokio::time::timeout(CLI_WAIT, ready.notified())
                .await
                .is_ok(),
        ),
    };
    if let Some(reached) = reached {
        if !reached {
            problems.push("CLI did not reach the requested cancellation barrier".to_string());
        }
        if let Err(error) = interrupt(child.id()) {
            problems.push(error.to_string());
        }
    }
    let status = match tokio::time::timeout(CLI_WAIT, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            problems.push("CLI exceeded its fixture deadline".into());
            let _ = interrupt(child.id());
            match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
                Ok(status) => status,
                Err(_) => {
                    let _ = child.start_kill();
                    child.wait().await
                }
            }
        }
    };
    // Actual child reaping closes the pipes; join both before propagating errors.
    let stdout = stdout.await;
    let stderr = stderr.await;
    let stdout = stdout??;
    let stderr = stderr??;
    if !problems.is_empty() {
        return Err(format!(
            "{}; actual child wait={status:?}; stdout={}; stderr={}",
            problems.join("; "),
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
        .into());
    }
    Ok(Output {
        status: status?,
        stdout,
        stderr,
    })
}
fn args(source: &Path, url: &str, tts: &Path, stt: &Path, stop: bool) -> Vec<String> {
    let mut args = vec![
        "run".into(),
        source.display().to_string(),
        url.into(),
        "--user-speech".into(),
        tts.display().to_string(),
        "--bot-audio-response".into(),
        stt.display().to_string(),
    ];
    if stop {
        args.push("--stop-bot".into());
    }
    args
}
fn assert_exit(out: &Output, code: i32) {
    assert_eq!(
        out.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    for key in [SECRET, STT_KEY] {
        assert!(!String::from_utf8_lossy(&out.stdout).contains(key));
        assert!(!String::from_utf8_lossy(&out.stderr).contains(key));
    }
}
async fn no_connection(listener: &TcpListener) -> bool {
    tokio::time::timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err()
}

#[cfg(not(all(feature = "user-speech", feature = "bot-audio-transcription")))]
#[tokio::test]
async fn absent_joint_feature_rejects_before_reading_missing_runtime_files_or_connecting() {
    let b = Bundle::new();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let out = cli(
        &args(
            &b.0.join("missing-source.yaml"),
            &format!("ws://{}", ws.local_addr().unwrap()),
            &b.0.join("missing-tts.json"),
            &b.0.join("missing-stt.json"),
            true,
        ),
        &b.0,
        CancelAt::Never,
    )
    .await
    .unwrap();
    let idle = no_connection(&ws).await;
    assert_exit(&out, 2);
    assert!(String::from_utf8_lossy(&out.stderr).contains("build feature"));
    assert!(out.stdout.is_empty());
    assert!(idle);
}

#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
mod enabled {
    use super::*;
    use crate::fixture::{
        self, assert_consumed, expectation, input_pcm, responses, source, stt_runtime, tts_runtime,
        Host, Http, Mode, Reply, LLM, SPOKEN,
    };
    use crate::input_fixture::{Http as TtsHttp, Reply as TtsReply};
    use common::REFERENCE;
    use pipecat_evals::matcher::ScriptResult;
    use serde_json::json;
    use std::sync::atomic::Ordering;

    fn results(out: &Output) -> Vec<ScriptResult> {
        // The existing run contract is one ScriptResult object per stdout line.
        // Decode every line strictly; a JSON array, blank or malformed line fails.
        let results: Vec<ScriptResult> = std::str::from_utf8(&out.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<ScriptResult>(line).unwrap())
            .collect();
        assert_eq!(
            results.len(),
            1,
            "each CLI fixture runs exactly one scenario"
        );
        results
    }

    #[tokio::test]
    async fn cold_and_warm_real_cli_processes_preserve_both_audio_settings_and_wav_override() {
        let b = Bundle::new();
        let cache = b.cache();
        let bytes = input_pcm();
        let recorded = fixture::PCM[28_000..31_914].to_vec();
        b.put("override.wav", input_fixture::wav(&recorded));
        let mut value = source();
        value["turns"] = json!([
            {"user":REFERENCE,"expect":[expectation("llm_response", LLM), expectation("response", SPOKEN)]},
            {"user":"WAV override reference","audio":"override.wav","expect":[expectation("response", "second spoken answer")]}
        ]);
        let source_path = b.json("case.json", &value);
        let tts = TtsHttp::start(vec![TtsReply::pcm(bytes.clone())]).await;
        let tts_config = b.json("tts.json", &tts_runtime(&tts.url, Some(&cache)));
        let mut tts_owner = Some(tts);
        let mut cold_requests = None;
        for warm in [false, true] {
            let stt = Http::start(
                vec![Reply::Stt(SPOKEN), Reply::Stt("second spoken answer")],
                false,
            )
            .await;
            let config = b.json("stt.json", &stt_runtime(&stt.url));
            let host = Host::start(vec![bytes.clone(), recorded.clone()], Mode::Normal).await;
            let out = cli(
                &args(&source_path, &host.url, &tts_config, &config, true),
                &b.0,
                CancelAt::Never,
            )
            .await;
            let (host_result, trace) = host.finish().await;
            let (stt_result, stt_trace) = stt.finish().await;
            if let Some(tts) = tts_owner.take() {
                cold_requests = Some(tts.finish().await);
            }
            host_result.unwrap();
            stt_result.unwrap();
            let out = out.unwrap();
            assert_exit(&out, 0);
            let results = results(&out);
            assert_eq!(results.len(), 1);
            assert!(results[0].passed, "warm={warm}: {:?}", results[0]);
            assert_eq!(responses(&results[0]), [SPOKEN, "second spoken answer"]);
            assert_consumed(&trace, &[bytes.clone(), recorded.clone()]);
            assert_eq!(stt_trace.requests.lock().unwrap().len(), 2);
        }
        let trace = cold_requests.unwrap();
        assert_eq!(trace.requests.lock().unwrap().len(), 1);
        input_fixture::assert_request(&trace.requests.lock().unwrap()[0], REFERENCE);
        input_fixture::assert_private_cache(&cache, &bytes);
    }

    #[tokio::test]
    async fn all_wav_cli_uses_joint_route_with_no_tts_request() {
        let b = Bundle::new();
        let bytes = input_pcm();
        b.put("input.wav", input_fixture::wav(&bytes));
        let mut value = source();
        value["user"] = json!({"modality":"audio"});
        value["turns"][0]["audio"] = "input.wav".into();
        let source_path = b.json("case.json", &value);
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts_config = b.json(
            "tts.json",
            &tts_runtime(&format!("http://{}", tts.local_addr().unwrap()), None),
        );
        let stt = Http::start(vec![Reply::Stt(SPOKEN)], false).await;
        let config = b.json("stt.json", &stt_runtime(&stt.url));
        let host = Host::start(vec![bytes.clone()], Mode::Normal).await;
        let destination = b.0.join("joint.wav");
        let mut arguments = args(&source_path, &host.url, &tts_config, &config, true);
        arguments.extend(["--recording-wav".into(), destination.display().to_string()]);
        let out = cli(&arguments, &b.0, CancelAt::Never).await;
        let (host_result, trace) = host.finish().await;
        let (stt_result, _) = stt.finish().await;
        let no_tts = no_connection(&tts).await;
        host_result.unwrap();
        stt_result.unwrap();
        let out = out.unwrap();
        assert_exit(&out, 0);
        assert!(results(&out)[0].passed);
        assert!(no_tts);
        assert_consumed(&trace, std::slice::from_ref(&bytes));
        let wav = std::fs::read(destination).unwrap();
        assert_eq!(&wav[..4], b"RIFF");
        assert!(wav[44..].windows(bytes.len()).any(|window| window == bytes));
    }

    #[tokio::test]
    async fn spoken_assertion_mismatch_is_exit_one_and_never_replaced_by_llm_decoy() {
        let b = Bundle::new();
        let bytes = input_pcm();
        let mut value = source();
        value["turns"][0]["expect"] =
            json!([{"event":"response","text_contains":LLM,"within_ms":3000}]);
        let source_path = b.json("case.json", &value);
        let tts = TtsHttp::start(vec![TtsReply::pcm(bytes.clone())]).await;
        let stt = Http::start(vec![Reply::Stt(SPOKEN)], false).await;
        let host = Host::start(vec![bytes.clone()], Mode::Normal).await;
        let tts_config = b.json("tts.json", &tts_runtime(&tts.url, None));
        let config = b.json("stt.json", &stt_runtime(&stt.url));
        let out = cli(
            &args(&source_path, &host.url, &tts_config, &config, true),
            &b.0,
            CancelAt::Never,
        )
        .await;
        let (host_result, trace) = host.finish().await;
        let _ = tts.finish().await;
        let (stt_result, _) = stt.finish().await;
        host_result.unwrap();
        stt_result.unwrap();
        let out = out.unwrap();
        assert_exit(&out, 1);
        let results = results(&out);
        assert!(!results[0].passed);
        assert_eq!(responses(&results[0]), [SPOKEN]);
        assert_consumed(&trace, &[bytes]);
    }

    #[tokio::test]
    async fn ctrl_c_during_stt_returns_cancelled_json_exit_one_and_client_http_eof() {
        let b = Bundle::new();
        let bytes = input_pcm();
        let source_path = b.json("case.json", &source());
        let tts = TtsHttp::start(vec![TtsReply::pcm(bytes.clone())]).await;
        let stt = Http::start(vec![Reply::Hold], false).await;
        let host = Host::start(vec![bytes], Mode::Normal).await;
        let tts_config = b.json("tts.json", &tts_runtime(&tts.url, None));
        let config = b.json("stt.json", &stt_runtime(&stt.url));
        let destination = b.0.join("cancelled-joint.wav");
        let mut arguments = args(&source_path, &host.url, &tts_config, &config, true);
        arguments.extend(["--recording-wav".into(), destination.display().to_string()]);
        let out = cli(&arguments, &b.0, CancelAt::Http(&stt.trace.received)).await;
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            while stt.trace.closed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let (host_result, trace) = host.finish().await;
        let tts_trace = tts.finish().await;
        let (stt_result, stt_trace) = stt.finish().await;
        host_result.unwrap();
        stt_result.unwrap();
        closed.unwrap();
        let out = out.unwrap();
        assert_exit(&out, 1);
        let results = results(&out);
        assert!(!results[0].passed);
        assert!(
            results[0].failures.iter().any(|f| f.kind == "cancelled"),
            "{:?}",
            results[0]
        );
        assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
        assert_eq!(tts_trace.active.load(Ordering::SeqCst), 0);
        assert_eq!(stt_trace.closed.load(Ordering::SeqCst), 1);
        assert_eq!(stt_trace.joined.load(Ordering::SeqCst), 1);
        assert_eq!(&std::fs::read(destination).unwrap()[..4], b"RIFF");
    }

    #[tokio::test]
    async fn joint_timeline_multiple_scenarios_reject_before_runtime_model_or_output_work() {
        let b = Bundle::new();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut second = source();
        second["name"] = "second valid joint scenario".into();
        let path = b.json(
            "multiple.json",
            &json!({"name":"collection", "scenarios":[source(), second]}),
        );
        let destination = b.0.join("never.wav");
        let mut arguments = args(
            &path,
            &format!("ws://{}", ws.local_addr().unwrap()),
            &b.0.join("missing-tts.json"),
            &b.0.join("missing-stt.json"),
            false,
        );
        arguments.extend(["--recording-wav".into(), destination.display().to_string()]);
        let mut before: Vec<_> = std::fs::read_dir(&b.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        let out = cli(&arguments, &b.0, CancelAt::Never).await.unwrap();
        assert_exit(&out, 2);
        assert!(String::from_utf8_lossy(&out.stderr).contains("exactly one scenario"));
        assert!(out.stdout.is_empty());
        assert!(no_connection(&ws).await);
        let mut after: Vec<_> = std::fs::read_dir(&b.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        before.sort();
        after.sort();
        assert_eq!(before, after, "rejection created an output temporary");
    }

    #[tokio::test]
    async fn joint_timeline_existing_destination_does_not_connect_or_clobber() {
        let b = Bundle::new();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_path = b.json("case.json", &source());
        let tts_config = b.json("tts.json", &tts_runtime("http://127.0.0.1:9", None));
        let config = b.json("stt.json", &stt_runtime("http://127.0.0.1:9"));
        let destination = b.put("existing.wav", b"keep original bytes");
        let mut arguments = args(
            &source_path,
            &format!("ws://{}", ws.local_addr().unwrap()),
            &tts_config,
            &config,
            true,
        );
        arguments.extend(["--recording-wav".into(), destination.display().to_string()]);
        let out = cli(&arguments, &b.0, CancelAt::Never).await.unwrap();
        assert_exit(&out, 2);
        assert!(out.stdout.is_empty());
        assert!(no_connection(&ws).await);
        assert_eq!(std::fs::read(destination).unwrap(), b"keep original bytes");
    }

    #[tokio::test]
    async fn invalid_joint_flags_later_scenario_and_settings_exit_two_without_network_or_cache() {
        let b = Bundle::new();
        let cache = b.cache();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", ws.local_addr().unwrap());
        let source_path = b.json("case.json", &source());
        let tts_config = b.json(
            "tts.json",
            &tts_runtime(
                &format!("http://{}", tts.local_addr().unwrap()),
                Some(&cache),
            ),
        );
        let stt_value = stt_runtime(&format!("http://{}", stt.local_addr().unwrap()));
        let config = b.json("stt.json", &stt_value);
        let base = args(&source_path, &url, &tts_config, &config, true);
        let extras = [
            vec!["--recorded-audio".into()],
            vec!["--user-images".into()],
            vec![
                "--bot-audio-wav".into(),
                b.0.join("never.wav").display().to_string(),
            ],
            vec!["--user-speech".into(), tts_config.display().to_string()],
            vec!["--bot-audio-response".into(), config.display().to_string()],
            vec!["--stop-bot".into()],
            vec!["--unknown".into()],
        ];
        for extra in extras {
            let mut arguments = base.clone();
            arguments.extend(extra);
            let out = cli(&arguments, &b.0, CancelAt::Never).await.unwrap();
            assert_exit(&out, 2);
            assert!(out.stdout.is_empty());
        }
        for flag in ["--user-speech", "--bot-audio-response"] {
            let arguments = vec![
                "run".into(),
                source_path.display().to_string(),
                url.clone(),
                flag.into(),
            ];
            let out = cli(&arguments, &b.0, CancelAt::Never).await.unwrap();
            assert_exit(&out, 2);
        }
        let suite = vec![
            "suite".into(),
            source_path.display().to_string(),
            "--user-speech".into(),
            tts_config.display().to_string(),
            "--bot-audio-response".into(),
            config.display().to_string(),
        ];
        assert_exit(&cli(&suite, &b.0, CancelAt::Never).await.unwrap(), 2);
        let mut bad = source();
        bad["name"] = "later bad".into();
        bad["turns"][0]["audio"] = "missing.wav".into();
        let bad_source = b.json(
            "later.json",
            &json!({"name":"preflight collection","scenarios":[source(), bad]}),
        );
        assert_exit(
            &cli(
                &args(&bad_source, &url, &tts_config, &config, false),
                &b.0,
                CancelAt::Never,
            )
            .await
            .unwrap(),
            2,
        );
        let mut other = source();
        other["name"] = "second valid scenario".into();
        let multiple = b.json(
            "multiple.json",
            &json!({"name":"stop bot cannot own two sessions","scenarios":[source(),other]}),
        );
        assert_exit(
            &cli(
                &args(&multiple, &url, &tts_config, &config, true),
                &b.0,
                CancelAt::Never,
            )
            .await
            .unwrap(),
            2,
        );
        let mut smart = stt_value.clone();
        smart["smart_turn_model"] = smart["silero_model"].clone();
        let smart_path = b.json("smart.json", &smart);
        assert_exit(
            &cli(
                &args(&source_path, &url, &tts_config, &smart_path, true),
                &b.0,
                CancelAt::Never,
            )
            .await
            .unwrap(),
            2,
        );
        let malformed = b.put("malformed.json", b"{not-json}");
        assert_exit(
            &cli(
                &args(&source_path, &url, &tts_config, &malformed, true),
                &b.0,
                CancelAt::Never,
            )
            .await
            .unwrap(),
            2,
        );
        let mut dtmf = source();
        dtmf["turns"] = json!([{"dtmf":"012#"}]);
        let dtmf_path = b.json("dtmf.json", &dtmf);
        assert_exit(
            &cli(
                &args(&dtmf_path, &url, &tts_config, &config, true),
                &b.0,
                CancelAt::Never,
            )
            .await
            .unwrap(),
            2,
        );
        assert!(no_connection(&ws).await && no_connection(&tts).await && no_connection(&stt).await);
        assert_eq!(std::fs::read_dir(cache).unwrap().count(), 0);
        assert!(!b.0.join("never.wav").exists());
    }
}
