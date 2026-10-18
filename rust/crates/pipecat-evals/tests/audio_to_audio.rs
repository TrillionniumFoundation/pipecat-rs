//! Source-shaped audio input -> actual native host -> captured speech -> genuine
//! local Silero -> inspected multipart STT -> spoken assertion / opt-in judge.
//! Synthetic transcript/judge replies test integration, never acoustic accuracy.
#![cfg(all(unix, feature = "user-speech", feature = "bot-audio-transcription"))]

#[allow(dead_code)]
#[path = "support/user_speech_common.rs"]
mod common;
#[allow(dead_code)]
#[path = "support/audio_to_audio_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/user_speech.rs"]
mod input_fixture;
use common::{Bundle, REFERENCE};
use fixture::*;
use input_fixture::{Http as TtsHttp, Reply as TtsReply};
use pipecat_evals::{
    AudioToAudioScenarioFile, Cancellation, EvalError, EvalSession, RecordedAudio, SessionOptions,
};
use serde_json::{json, Value};
use std::{path::Path, sync::atomic::Ordering, time::Duration};
use tokio::net::TcpListener;

// Keep setup failures owned too: all started loopback servers are joined before
// reporting a rejected supposedly-valid fixture. No production owner is replaced.
struct Case {
    host: Host,
    tts: TtsHttp,
    stt: Http,
    session: EvalSession,
}
impl Case {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        b: &Bundle,
        value: &Value,
        clips: Vec<Vec<u8>>,
        mode: Mode,
        tts_replies: Vec<TtsReply>,
        stt_replies: Vec<Reply>,
        cache: Option<&Path>,
        opts: SessionOptions,
    ) -> Self {
        let tts = TtsHttp::start(tts_replies).await;
        let stt = Http::start(stt_replies, false).await;
        let host = Host::start(clips, mode).await;
        let session = prepare(
            b,
            value,
            &host.url,
            tts_settings(&tts.url, cache, Duration::from_secs(2)),
            stt_settings(b, &stt.url),
            opts,
        )
        .await;
        match session {
            Ok(session) => Self {
                host,
                tts,
                stt,
                session,
            },
            Err(error) => {
                let _ = host.finish().await;
                let _ = tts.finish().await;
                let _ = stt.finish().await;
                panic!("joint fixture preparation failed after owner joins: {error}");
            }
        }
    }
}

#[tokio::test]
async fn complete_native_route_waits_for_host_consumption_and_only_stt_satisfies_response() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let judge_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut value = source();
    // Configured but unnecessary judge must make zero requests.
    value["judge"] = judge_settings(&format!("http://{}", judge_listener.local_addr().unwrap()));
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Stt(SPOKEN)],
        None,
        options(),
    )
    .await;
    host.trace.gated.store(true, Ordering::SeqCst);
    let cancel = Cancellation::new();
    let task = tokio::spawn(session.run(cancel.clone()));
    let tail = tokio::time::timeout(WAIT, host.trace.tail.notified()).await;
    let returned_before_complete_consumption = task.is_finished();
    let emitted_before_consumption = host.trace.emitted.load(Ordering::SeqCst);
    host.trace.release.add_permits(1);
    let result = joined(task, &cancel).await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    let no_judge = tokio::time::timeout(Duration::from_millis(30), judge_listener.accept())
        .await
        .is_err();
    tail.unwrap();
    assert!(!returned_before_complete_consumption);
    assert_eq!(emitted_before_consumption, 0);
    host_result.unwrap();
    stt_result.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), [SPOKEN]);
    assert!(result
        .events_seen
        .iter()
        .any(|e| e["type"] == "llm_response" && e["text"] == LLM));
    assert_consumed(&trace, &[bytes]);
    assert_eq!(tts_trace.requests.lock().unwrap().len(), 1);
    input_fixture::assert_request(&tts_trace.requests.lock().unwrap()[0], REFERENCE);
    assert_eq!(stt_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(stt_trace.joined.load(Ordering::SeqCst), 1);
    assert!(
        no_judge,
        "exact/regex assertions unexpectedly enabled semantic judging"
    );
    assert_private(&result);
}

#[tokio::test]
async fn real_opt_in_judge_receives_reference_and_actual_spoken_text() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let judge = Http::start(vec![Reply::Judge("yes")], true).await;
    let mut value = source();
    value["judge"] = judge_settings(&judge.url);
    value["turns"][0]["expect"] =
        json!([{"event":"response","eval":"satisfies the spoken criterion","within_ms":7000}]);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Stt(SPOKEN)],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let _ = tts.finish().await;
    let (stt_result, _) = stt.finish().await;
    let (judge_result, judge_trace) = judge.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    judge_result.unwrap();
    assert!(result.passed, "{result:?}");
    assert_consumed(&trace, &[bytes]);
    let requests = judge_trace.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let body = request_json(&requests[0]);
    assert_eq!(
        body["state"],
        json!({"conversation":[{"speaker":"user","text":REFERENCE}],"latest_bot_reply":SPOKEN})
    );
    assert!(!body.to_string().contains(LLM));
    assert!(!body.to_string().contains(TTS));
    assert_eq!(
        result.turns[0].expectations[0].judgments[0].decision,
        pipecat_evals::JudgeDecision::Yes
    );
    assert_private(&result);
}

#[tokio::test]
async fn matching_llm_and_reference_cannot_pass_without_actual_bot_pcm() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let mut value = source();
    value["turns"][0]["user"] = LLM.into();
    value["turns"][0]["expect"] = json!([{"event":"response","text_contains":LLM,"within_ms":500}]);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Silent,
        vec![TtsReply::pcm(bytes.clone())],
        vec![],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let _ = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    assert!(!result.passed);
    assert!(responses(&result).is_empty());
    assert!(stt_trace.requests.lock().unwrap().is_empty());
    assert_consumed(&trace, &[bytes]);
    assert_private(&result);
}

#[tokio::test]
async fn cold_then_warm_session_uses_cache_and_recording_override_without_second_tts() {
    let b = Bundle::new();
    let cache = b.cache();
    let bytes = input_pcm();
    let recorded = PCM[28_000..31_914].to_vec();
    b.put("override.wav", input_fixture::wav(&recorded));
    let mut value = source();
    value["turns"] = json!([
        {"user":REFERENCE,"expect":[expectation("response", SPOKEN)]},
        {"user":"recorded reference remains context only","audio":"override.wav","expect":[expectation("response", "second spoken answer")]}
    ]);
    let tts = TtsHttp::start(vec![TtsReply::pcm(bytes.clone())]).await;
    let base = tts.url.clone();
    let mut cold_trace = None;
    let mut tts_owner = Some(tts);
    for warm in [false, true] {
        let stt = Http::start(
            vec![Reply::Stt(SPOKEN), Reply::Stt("second spoken answer")],
            false,
        )
        .await;
        let host = Host::start(vec![bytes.clone(), recorded.clone()], Mode::Normal).await;
        let prepared = prepare(
            &b,
            &value,
            &host.url,
            tts_settings(&base, Some(&cache), Duration::from_secs(2)),
            stt_settings(&b, &stt.url),
            options(),
        )
        .await;
        let result = match prepared {
            Ok(session) => Some(run(session).await),
            Err(_) => None,
        };
        let (host_result, trace) = host.finish().await;
        let (stt_result, stt_trace) = stt.finish().await;
        if let Some(tts) = tts_owner.take() {
            cold_trace = Some(tts.finish().await);
        }
        host_result.unwrap();
        stt_result.unwrap();
        let result = result.expect("valid cache joint preparation");
        assert!(result.passed, "warm={warm}; {result:?}");
        assert_consumed(&trace, &[bytes.clone(), recorded.clone()]);
        assert_eq!(stt_trace.requests.lock().unwrap().len(), 2);
        assert_private(&result);
    }
    let tts_trace = cold_trace.unwrap();
    assert_eq!(tts_trace.requests.lock().unwrap().len(), 1);
    input_fixture::assert_request(&tts_trace.requests.lock().unwrap()[0], REFERENCE);
    input_fixture::assert_private_cache(&cache, &bytes);
}

#[tokio::test]
async fn all_wav_joint_plan_needs_no_speech_owner_and_freezes_recordings() {
    let b = Bundle::new();
    let bytes = input_pcm();
    b.put("input.wav", input_fixture::wav(&bytes));
    let mut value = source();
    value["user"] = json!({"modality":"audio"});
    value["turns"][0]["audio"] = "input.wav".into();
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Normal,
        vec![],
        vec![Reply::Stt(SPOKEN)],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, _) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    assert!(result.passed, "{result:?}");
    assert!(tts_trace.requests.lock().unwrap().is_empty());
    assert_consumed(&trace, std::slice::from_ref(&bytes));
    let source_path = b.json("frozen.json", &value);
    let mut sessions = AudioToAudioScenarioFile::load(source_path)
        .unwrap()
        .into_sessions_with_cancellation(
            "ws://127.0.0.1:9",
            options(),
            tts_settings("http://127.0.0.1:9", None, Duration::from_secs(1)),
            stt_settings(&b, "http://127.0.0.1:9"),
            Cancellation::new(),
        )
        .await
        .unwrap();
    let replacement = sessions.remove(0).with_recorded_audio_inputs(vec![(
        0,
        RecordedAudio::from_wav(&input_fixture::wav(&bytes)).unwrap(),
    )]);
    assert!(
        replacement.is_err(),
        "frozen combined recordings accepted replacement"
    );
}

#[tokio::test]
async fn interruption_during_input_does_not_revoke_complete_user_utterance() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &source(),
        vec![bytes.clone()],
        Mode::InterruptInput,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Stt(SPOKEN)],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let _ = tts.finish().await;
    let (stt_result, _) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    assert!(result.passed, "{result:?}");
    assert_consumed(&trace, &[bytes]);
    assert_eq!(responses(&result), [SPOKEN]);
}

#[tokio::test]
async fn final_llm_assertion_still_settles_admitted_pcm_and_pending_stt() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let mut value = source();
    value["turns"][0]["expect"] = json!([expectation("llm_response", LLM)]);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Hold],
        None,
        options(),
    )
    .await;
    let cancel = Cancellation::new();
    let task = tokio::spawn(session.run(cancel.clone()));
    let arrived = tokio::time::timeout(WAIT, stt.trace.received.notified()).await;
    let finished_before_stt = task.is_finished();
    stt.trace.released.add_permits(1);
    let result = joined(task, &cancel).await;
    let (host_result, trace) = host.finish().await;
    let _ = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    arrived.unwrap();
    host_result.unwrap();
    stt_result.unwrap();
    assert!(
        !finished_before_stt,
        "LLM assertion skipped admitted-prefix settlement"
    );
    assert!(result.passed, "{result:?}");
    assert_consumed(&trace, &[bytes]);
    assert_eq!(stt_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(responses(&result), ["stale forbidden transcript"]);
}

#[tokio::test]
async fn final_settlement_has_its_own_fixed_timeout_and_closes_pending_http() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let mut value = source();
    value["turns"][0]["expect"] = json!([expectation("llm_response", LLM)]);
    let mut opts = options();
    opts.bot_quiet_timeout = Duration::from_millis(2500);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Hold],
        None,
        opts,
    )
    .await;
    let result = run(session).await;
    // Observe the client-initiated HTTP EOF before fixture shutdown is requested.
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        while stt.trace.closed.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (host_result, _) = host.finish().await;
    let _ = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    observed.unwrap();
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.kind == "bot_transcription_timeout"),
        "{result:?}"
    );
    assert_eq!(stt_trace.closed.load(Ordering::SeqCst), 1);
    assert_private(&result);
}

#[tokio::test]
async fn cancellation_during_stt_closes_peer_before_fixture_shutdown_and_joins_owners() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &source(),
        vec![bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Hold],
        None,
        options(),
    )
    .await;
    let cancel = Cancellation::new();
    let task = tokio::spawn(session.run(cancel.clone()));
    let arrived = tokio::time::timeout(WAIT, stt.trace.received.notified()).await;
    cancel.cancel();
    let result = joined(task, &cancel).await;
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while stt.trace.closed.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    arrived.unwrap();
    closed.unwrap();
    host_result.unwrap();
    stt_result.unwrap();
    assert!(!result.passed);
    assert!(
        result.failures.iter().any(|f| f.kind == "cancelled"),
        "{result:?}"
    );
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(tts_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(stt_trace.joined.load(Ordering::SeqCst), 1);
    assert_eq!(stt_trace.closed.load(Ordering::SeqCst), 1);
    assert_private(&result);
}

#[tokio::test]
async fn malformed_later_scenario_and_pre_cancel_leave_all_peers_and_cache_untouched() {
    let b = Bundle::new();
    let cache = b.cache();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", ws.local_addr().unwrap());
    let tts_url = format!("http://{}", tts.local_addr().unwrap());
    let stt_url = format!("http://{}", stt.local_addr().unwrap());
    let mut bad = source();
    bad["name"] = "bad later recording".into();
    bad["turns"][0]["audio"] = "missing.wav".into();
    let path = b.json(
        "later-invalid.json",
        &json!({"name":"all scenarios preflight","scenarios":[source(),bad]}),
    );
    assert!(AudioToAudioScenarioFile::load(path).is_err());
    let cancel = Cancellation::new();
    cancel.cancel();
    let outcome = AudioToAudioScenarioFile::load(b.json("valid.json", &source()))
        .unwrap()
        .into_sessions_with_cancellation(
            &url,
            options(),
            tts_settings(&tts_url, Some(&cache), Duration::from_secs(1)),
            stt_settings(&b, &stt_url),
            cancel,
        )
        .await;
    assert!(matches!(outcome, Err(EvalError::Cancelled)));
    for listener in [&ws, &tts, &stt] {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }
    assert_eq!(std::fs::read_dir(cache).unwrap().count(), 0);
}

#[tokio::test]
async fn source_send_after_response_is_spoken_and_survives_joint_construction() {
    let b = Bundle::new();
    let bytes = input_pcm();
    let mut value = source();
    value["turns"] = json!([
        {"user":REFERENCE,"expect":[expectation("llm_response", LLM)]},
        {"user":"second audio reference","send_after":{"event":"response"},"expect":[expectation("response", "second spoken answer")]}
    ]);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone(), bytes.clone()],
        Mode::Normal,
        vec![TtsReply::pcm(bytes.clone()), TtsReply::pcm(bytes.clone())],
        vec![Reply::Stt(SPOKEN), Reply::Stt("second spoken answer")],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, _) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    assert!(result.passed, "{result:?}");
    assert_consumed(&trace, &[bytes.clone(), bytes]);
    assert_eq!(tts_trace.requests.lock().unwrap().len(), 2);
    assert_eq!(responses(&result), [SPOKEN, "second spoken answer"]);
}

#[tokio::test]
async fn input_production_failure_remains_terminal_with_stop_on_failure_false() {
    let b = Bundle::new();
    let mut value = source();
    value["stop_on_failure"] = false.into();
    value["turns"] = json!([
        {"user":REFERENCE,"expect":[expectation("response", SPOKEN)]},
        {"user":"must never synthesize this next turn","expect":[expectation("response", SPOKEN)]}
    ]);
    let mut invalid = TtsReply::pcm(vec![1, 2, 3]);
    invalid.status = 503;
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![],
        Mode::Normal,
        vec![invalid],
        vec![],
        None,
        options(),
    )
    .await;
    let result = run(session).await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.kind == "user_speech_error"),
        "{result:?}"
    );
    assert_eq!(tts_trace.requests.lock().unwrap().len(), 1);
    assert!(stt_trace.requests.lock().unwrap().is_empty());
    assert_eq!(trace.completed.load(Ordering::SeqCst), 0);
    assert_private(&result);
}

#[tokio::test]
async fn early_spoken_match_waits_for_local_input_and_cache_completion() {
    let b = Bundle::new();
    let cache = b.cache();
    let bytes = input_pcm().repeat(40);
    let expected_packets = input_fixture::packets(&bytes);
    let mut value = source();
    value["turns"][0]["expect"] = json!([expectation("response", SPOKEN)]);
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &value,
        vec![bytes.clone()],
        Mode::Early,
        vec![TtsReply::pcm(bytes.clone())],
        vec![Reply::Stt(SPOKEN)],
        Some(&cache),
        options(),
    )
    .await;
    let retired_tts_url = tts.url.clone();
    let cancel = Cancellation::new();
    let started = tokio::time::Instant::now();
    let task = tokio::spawn(session.run(cancel.clone()));
    let arrived = tokio::time::timeout(WAIT, stt.trace.received.notified()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let early_return = task.is_finished();
    let consumed_at_early_response = host.trace.completed.load(Ordering::SeqCst);
    let result = joined(task, &cancel).await;
    let elapsed = started.elapsed();
    let cache_files = input_fixture::cache_wavs(&cache);
    let cache_exact_at_run_return = cache_files.len() == 1
        && std::fs::read(&cache_files[0])
            .ok()
            .and_then(|wav| RecordedAudio::from_wav(&wav).ok())
            .is_some_and(|audio| audio.pcm_bytes() == bytes);
    // The run's last paced-send receipt is local admission. Let this endpoint
    // retire naturally: fixture cancellation must not race its queued input.
    // Even natural retirement does not promise that its processor saw the tail.
    let (host_result, trace) = host.finish_without_cancel().await;
    let tts_trace = tts.finish().await;
    let (stt_result, _) = stt.finish().await;
    arrived.unwrap();
    host_result.unwrap();
    stt_result.unwrap();
    assert!(
        !early_return,
        "early spoken match cut off local paced input"
    );
    assert_eq!(consumed_at_early_response, 0);
    assert!(result.passed, "{result:?}");
    // Supporting pacing evidence, not a remote receipt or an exact observation
    // of when the evaluator consumed the earlier synthetic STT result.
    assert!(elapsed >= Duration::from_millis((expected_packets.len() as u64 - 1) * 40));
    assert!(cache_exact_at_run_return);
    input_fixture::assert_private_cache(&cache, &bytes);
    assert_eq!(responses(&result), [SPOKEN]);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert!(trace.errors.lock().unwrap().is_empty());
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let consumed: Vec<_> = trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .filter(|packet| packet.iter().any(|b| *b != 0))
        .cloned()
        .collect();
    assert!(!consumed.is_empty());
    assert!(consumed.len() <= expected_packets.len());
    assert_eq!(
        consumed,
        expected_packets[..consumed.len()],
        "observed host PCM must be an exact, unreordered prefix"
    );
    assert_eq!(tts_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(tts_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(tts_trace.joined.load(Ordering::SeqCst), 1);

    // Independent complete-consumer evidence for the identical long clip: the
    // cold HTTP listener and handler are now joined/closed. A fresh warm session
    // can receive bot PCM only after this host validates every input packet.
    let mut warm_source = value;
    warm_source["turns"][0]["expect"][0]["within_ms"] = 12_000.into();
    let stt = Http::start(vec![Reply::Stt(SPOKEN)], false).await;
    let host = Host::start(vec![bytes.clone()], Mode::Normal).await;
    let prepared = prepare(
        &b,
        &warm_source,
        &host.url,
        tts_settings(&retired_tts_url, Some(&cache), Duration::from_secs(2)),
        stt_settings(&b, &stt.url),
        options(),
    )
    .await;
    let warm = match prepared {
        Ok(session) => Ok(run(session).await),
        Err(error) => Err(error),
    };
    let (host_result, trace) = host.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    host_result.unwrap();
    stt_result.unwrap();
    let warm = warm.unwrap();
    assert!(warm.passed, "{warm:?}");
    assert_consumed(&trace, std::slice::from_ref(&bytes));
    assert_eq!(responses(&warm), [SPOKEN]);
    assert_eq!(stt_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(
        tts_trace.requests.lock().unwrap().len(),
        1,
        "warm run cannot contact the retired TTS listener"
    );
    input_fixture::assert_private_cache(&cache, &bytes);
}

#[tokio::test]
async fn cancellation_during_tts_body_closes_its_http_peer_without_sending_partial_pcm() {
    let b = Bundle::new();
    let mut stalled = TtsReply::pcm(input_pcm());
    stalled.stalled = true;
    let Case {
        host,
        tts,
        stt,
        session,
    } = Case::new(
        &b,
        &source(),
        vec![],
        Mode::Normal,
        vec![stalled],
        vec![],
        None,
        options(),
    )
    .await;
    let cancel = Cancellation::new();
    let task = tokio::spawn(session.run(cancel.clone()));
    let arrived = tokio::time::timeout(WAIT, tts.trace.admitted.notified()).await;
    // Admission alone precedes the server's body write. Give it time to enter
    // the held body read, so its observed EOF cannot be server cancellation.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    let result = joined(task, &cancel).await;
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while tts.trace.closed.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let (host_result, trace) = host.finish().await;
    let tts_trace = tts.finish().await;
    let (stt_result, stt_trace) = stt.finish().await;
    arrived.unwrap();
    closed.unwrap();
    host_result.unwrap();
    stt_result.unwrap();
    assert!(!result.passed);
    assert!(
        result.failures.iter().any(|f| f.kind == "cancelled"),
        "{result:?}"
    );
    assert_eq!(trace.completed.load(Ordering::SeqCst), 0);
    assert!(trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .all(|p| p.iter().all(|b| *b == 0)));
    assert_eq!(tts_trace.closed.load(Ordering::SeqCst), 1);
    assert_eq!(tts_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(tts_trace.joined.load(Ordering::SeqCst), 1);
    assert!(stt_trace.requests.lock().unwrap().is_empty());
}
