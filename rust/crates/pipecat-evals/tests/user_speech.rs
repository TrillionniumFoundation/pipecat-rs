//! Independent complete-route acceptance for the explicitly selected Cartesia input.
//! All audio and credentials are synthetic; no ambient credentials are read.
#![cfg(all(unix, feature = "user-speech"))]

#[path = "support/user_speech_common.rs"]
mod common;
use common::*;

#[path = "support/user_speech.rs"]
mod fixture;
use fixture::*;
use pipecat_evals::{
    matcher::ScriptResult, Cancellation, CartesiaUserSpeechSettings, CartesiaUserSpeechVoice,
    EvalSession, RecordedAudio, RecordedScenarioFile, ScenarioFile, ScriptScenario, SessionOptions,
    UserSpeechCachePolicy, UserSpeechScenarioFile,
};
use pipecat_speech::SpeechConfig;
use pipecat_transports::{Packet, WebSocketSession};
use serde_json::{json, Value};
use std::{path::Path, sync::atomic::Ordering, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};

fn options() -> SessionOptions {
    SessionOptions {
        stop_bot: true,
        default_timeout: Duration::from_secs(4),
        handshake_timeout: Duration::from_secs(2),
        bot_quiet_timeout: Duration::from_secs(2),
        max_duration: WAIT,
        transport: transport(),
        ..Default::default()
    }
}

fn settings(base: &str, cache: Option<&Path>, timeout: Duration) -> CartesiaUserSpeechSettings {
    assert!(
        base.starts_with("http://127.0.0.1:"),
        "only synthetic loopback fixtures"
    );
    let mut config = SpeechConfig::cartesia(SECRET);
    config.base_url = base.into();
    config.timeout = timeout;
    let cache = cache.map_or(UserSpeechCachePolicy::Disabled, |directory| {
        UserSpeechCachePolicy::Private {
            directory: directory.into(),
            max_entries: 8,
            max_bytes: 16 * 1024 * 1024,
        }
    });
    CartesiaUserSpeechSettings::new(config, cache).unwrap()
}
fn voice() -> CartesiaUserSpeechVoice {
    CartesiaUserSpeechVoice::new("Fixture-VOICE", None, None).unwrap()
}
fn scenario(turns: Value) -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"independent user speech", "turns":turns}))
        .unwrap()
        .scenarios
        .remove(0)
}
fn one() -> ScriptScenario {
    scenario(json!([{"user":REFERENCE,"expect":[expected(1)]}]))
}
fn session(
    scenario: ScriptScenario,
    url: &str,
    settings: CartesiaUserSpeechSettings,
) -> EvalSession {
    EvalSession::new(scenario, url, options())
        .unwrap()
        .with_cartesia_user_speech(settings, voice())
        .unwrap()
}
async fn joined(mut task: JoinHandle<ScriptResult>, cancel: &Cancellation) -> ScriptResult {
    match tokio::time::timeout(WAIT + Duration::from_secs(2), &mut task).await {
        Ok(result) => result.expect("session task panicked"),
        Err(_) => {
            cancel.cancel();
            let actual = task.await;
            panic!("session deadline after actual joined cleanup: {actual:?}")
        }
    }
}
async fn run(session: EvalSession) -> ScriptResult {
    let cancel = Cancellation::new();
    let c = cancel.clone();
    joined(tokio::spawn(async move { session.run(c).await }), &cancel).await
}

#[tokio::test]
async fn no_cache_full_native_host_consumption_waits_for_delayed_tail_and_never_sends_text() {
    let b = Bundle::new();
    let unused_cache = b.cache();
    b.put("unrelated-cache-marker", b"must remain untouched");
    let bytes = pcm(17);
    let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let host = Host::start(vec![bytes.clone()]).await;
    host.trace.gated.store(true, Ordering::SeqCst);
    let configured = session(
        one(),
        &host.url,
        settings(&http.url, None, Duration::from_secs(2)),
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { configured.run(c).await });
    let arrived = tokio::time::timeout(WAIT, host.trace.final_arrived.notified())
        .await
        .is_ok();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let returned_before_consumption = task.is_finished();
    let completed_before_release = host.trace.completed.load(Ordering::SeqCst);
    host.trace.final_gate.add_permits(1);
    let result = joined(task, &cancel).await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(
        arrived,
        "complete tail never reached actual host pipeline: {result:?}"
    );
    assert!(
        !returned_before_consumption,
        "sender admission incorrectly became remote acceptance"
    );
    assert_eq!(completed_before_release, 0);
    assert!(result.passed, "{result:?}");
    assert!(report.is_ok(), "{report:?}");
    assert_consumed(&trace, &[bytes]);
    assert_eq!(std::fs::read_dir(unused_cache).unwrap().count(), 0);
    assert_eq!(
        std::fs::read(b.0.join("unrelated-cache-marker")).unwrap(),
        b"must remain untouched"
    );
    let requests = http_trace.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_request(&requests[0], REFERENCE);
    assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(http_trace.joined.load(Ordering::SeqCst), 1);
    assert_redacted(&result);
    // Production capability remains deliberately weaker than this fixture receipt.
    assert!(result
        .events_seen
        .iter()
        .all(|event| event["text"] != REFERENCE));
}

#[tokio::test]
async fn cold_then_warm_new_session_replays_complete_pcm_after_http_listener_has_closed() {
    let b = Bundle::new();
    let root = b.cache();
    let bytes = pcm(29);
    let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let configured = settings(&http.url, Some(&root), Duration::from_secs(2));
    let cold_host = Host::start(vec![bytes.clone()]).await;
    let cold = run(session(one(), &cold_host.url, configured.clone())).await;
    let (cold_report, cold_trace) = cold_host.finish().await;
    let http_trace = http.finish().await;
    // No listener or HTTP task remains while the separately constructed warm session runs.
    let warm_host = Host::start(vec![bytes.clone()]).await;
    let warm = run(session(one(), &warm_host.url, configured)).await;
    let (warm_report, warm_trace) = warm_host.finish().await;
    assert!(cold.passed && warm.passed, "cold={cold:?}; warm={warm:?}");
    assert!(cold_report.is_ok() && warm_report.is_ok());
    assert_consumed(&cold_trace, std::slice::from_ref(&bytes));
    assert_consumed(&warm_trace, std::slice::from_ref(&bytes));
    assert_private_cache(&root, &bytes);
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_loader_preserves_recording_override_and_observation_without_extra_synthesis() {
    let b = Bundle::new();
    let synthesized = pcm(41);
    let recorded = pcm(61);
    b.put("override.wav", wav(&recorded));
    let mut value = source();
    value["turns"] = json!([
        {"user":REFERENCE,"expect":[expected(1)]},
        {"expect":[{"event":"user_transcription","absent":true,"within_ms":50}]},
        {"user":"recorded reference must not be synthesized","audio":"override.wav","expect":[expected(2)]}
    ]);
    let path = b.json("mixed.json", &value);
    let http = Http::start(vec![Reply::pcm(synthesized.clone())]).await;
    let host = Host::start(vec![synthesized.clone(), recorded.clone()]).await;
    let loaded = UserSpeechScenarioFile::load(&path).unwrap();
    let mut sessions = loaded
        .into_sessions(
            &host.url,
            options(),
            settings(&http.url, None, Duration::from_secs(2)),
        )
        .unwrap();
    let result = run(sessions.remove(0)).await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(result.passed, "{result:?}");
    assert!(report.is_ok(), "{report:?}");
    assert_consumed(&trace, &[synthesized, recorded]);
    let requests = http_trace.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_request(&requests[0], REFERENCE);
    assert!(ScenarioFile::load(&path).is_err());
    assert!(RecordedScenarioFile::load(&path).is_err());
}

#[tokio::test]
async fn explicit_recording_wins_in_both_public_builder_orders() {
    for recording_first in [false, true] {
        let bytes = pcm(81);
        let http = Http::start(vec![]).await;
        let host = Host::start(vec![bytes.clone()]).await;
        let base = EvalSession::new(one(), &host.url, options()).unwrap();
        let settings = settings(&http.url, None, Duration::from_secs(2));
        let recording = RecordedAudio::from_pcm16(&bytes).unwrap();
        let session = if recording_first {
            base.with_recorded_audio_inputs([(0, recording)])
                .unwrap()
                .with_cartesia_user_speech(settings, voice())
                .unwrap()
        } else {
            base.with_cartesia_user_speech(settings, voice())
                .unwrap()
                .with_recorded_audio_inputs([(0, recording)])
                .unwrap()
        };
        let result = run(session).await;
        let (report, trace) = host.finish().await;
        let http_trace = http.finish().await;
        assert!(
            result.passed,
            "recording_first={recording_first}: {result:?}"
        );
        assert!(report.is_ok());
        assert_consumed(&trace, &[bytes]);
        assert!(http_trace.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn every_non_200_and_invalid_complete_body_discards_audio_and_never_publishes_cache() {
    let valid = pcm(101);
    let mut cases: Vec<(&str, Reply)> = [201, 206, 400, 401, 429, 500]
        .into_iter()
        .map(|status| {
            let mut reply = Reply::pcm(valid.clone());
            reply.status = status;
            (
                match status {
                    201 => "201",
                    206 => "206",
                    400 => "400",
                    401 => "401",
                    429 => "429",
                    _ => "500",
                },
                reply,
            )
        })
        .collect();
    cases.push(("empty", Reply::pcm(vec![])));
    cases.push(("partial sample", Reply::pcm(vec![1, 0, 2])));
    let mut truncated = Reply::pcm(valid.clone());
    truncated.truncated = true;
    cases.push(("truncated content length", truncated));
    let mut mime = Reply::pcm(b"provider body sentinel".to_vec());
    mime.mime = "application/json";
    cases.push(("wrong MIME", mime));
    cases.push(("oversized chunked body", Reply::pcm(vec![0x17; 1_920_002])));
    for (name, reply) in cases {
        let b = Bundle::new();
        let root = b.cache();
        let http = Http::start(vec![reply]).await;
        let host = Host::start(vec![valid.clone()]).await;
        let result = run(session(
            one(),
            &host.url,
            settings(&http.url, Some(&root), Duration::from_secs(5)),
        ))
        .await;
        let (report, trace) = host.finish().await;
        let http_trace = http.finish().await;
        assert!(!result.passed, "{name}: {result:?}");
        assert!(
            result
                .failures
                .iter()
                .any(|f| matches!(f.kind.as_str(), "user_speech_error" | "limit_exceeded")),
            "{name}: wrong error attribution {result:?}"
        );
        assert!(report.is_ok(), "{name}: {report:?}");
        assert_no_speech(&trace);
        assert_no_cached_audio(&root);
        assert_eq!(
            http_trace.requests.lock().unwrap().len(),
            1,
            "{name}: no automatic retries"
        );
        assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
        assert_eq!(http_trace.joined.load(Ordering::SeqCst), 1);
        assert_redacted(&result);
    }
}

#[tokio::test]
async fn silent_http_production_deadline_is_fixed_even_when_bot_events_continue() {
    for chatty in [false, true] {
        let b = Bundle::new();
        let root = b.cache();
        let mut reply = Reply::pcm(vec![]);
        reply.stalled = true;
        let http = Http::start(vec![reply]).await;
        let host = Host::start(vec![pcm(7)]).await;
        host.trace.chatty.store(chatty, Ordering::SeqCst);
        let scenario = scenario(json!([
            {"user":REFERENCE,"expect":[expected(1)]},
            {"user":"next turn must never synthesize","expect":[expected(2)]}
        ]));
        let now = tokio::time::Instant::now();
        let result = run(session(
            scenario,
            &host.url,
            settings(&http.url, Some(&root), Duration::from_millis(180)),
        ))
        .await;
        let elapsed = now.elapsed();
        let closed_before_fixture_stop = tokio::time::timeout(Duration::from_secs(1), async {
            while http.trace.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let (report, trace) = host.finish().await;
        let http_trace = http.finish().await;
        assert!(!result.passed, "chatty={chatty}: {result:?}");
        assert!(
            result
                .failures
                .iter()
                .any(|f| f.kind == "user_speech_timeout"),
            "producer timeout swallowed or relabelled: {result:?}"
        );
        assert!(
            elapsed < Duration::from_millis(1500),
            "incoming events reset production deadline: {elapsed:?}"
        );
        assert!(
            closed_before_fixture_stop,
            "HTTP was not closed by session cleanup"
        );
        assert_eq!(http_trace.closed.load(Ordering::SeqCst), 1);
        assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
        assert_no_cached_audio(&root);
        assert!(report.is_ok());
        assert_no_speech(&trace);
        assert_eq!(result.turns[1].status, "not_run");
    }
}

#[tokio::test]
async fn assertion_deadline_starts_before_http_instead_of_after_synthesis() {
    let mut reply = Reply::pcm(pcm(37));
    reply.delay = Duration::from_millis(250);
    let http = Http::start(vec![reply]).await;
    let host = Host::start(vec![pcm(37)]).await;
    let scenario = scenario(
        json!([{"user":REFERENCE,"expect":[{"event":"llm_response","text_contains":"complete PCM 1","within_ms":70}]}]),
    );
    let result = run(session(
        scenario,
        &host.url,
        settings(&http.url, None, Duration::from_secs(2)),
    ))
    .await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(
        !result.passed,
        "slow HTTP improperly moved assertion anchor: {result:?}"
    );
    assert!(
        result.failures.iter().any(|f| f.kind == "timeout"),
        "{result:?}"
    );
    assert!(
        !result
            .failures
            .iter()
            .any(|f| f.kind == "user_speech_timeout"),
        "assertion timeout was relabelled"
    );
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert!(report.is_ok());
}

#[tokio::test]
async fn cancellation_joins_stalled_http_and_does_not_play_or_cache_partial_body() {
    let b = Bundle::new();
    let root = b.cache();
    let mut reply = Reply::pcm(pcm(13));
    reply.stalled = true;
    let http = Http::start(vec![reply]).await;
    let host = Host::start(vec![pcm(13)]).await;
    let session = session(
        one(),
        &host.url,
        settings(&http.url, Some(&root), Duration::from_secs(4)),
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { session.run(c).await });
    let admitted = tokio::time::timeout(WAIT, http.trace.admitted.notified())
        .await
        .is_ok();
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    let result = joined(task, &cancel).await;
    let closed_before_fixture_stop = tokio::time::timeout(Duration::from_secs(1), async {
        while http.trace.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(admitted);
    assert!(!result.passed);
    assert!(
        result.failures.iter().any(|f| f.kind == "cancelled"),
        "{result:?}"
    );
    assert!(
        closed_before_fixture_stop,
        "session failed to close owned HTTP transport"
    );
    assert_eq!(http_trace.closed.load(Ordering::SeqCst), 1);
    assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(http_trace.joined.load(Ordering::SeqCst), 1);
    assert_no_cached_audio(&root);
    assert!(report.is_ok());
    assert_no_speech(&trace);
}

#[tokio::test]
async fn early_success_still_waits_for_complete_local_playback_and_cache_settlement() {
    let b = Bundle::new();
    let root = b.cache();
    let bytes = pcm(87);
    let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let host = Host::start(vec![bytes.clone()]).await;
    host.trace.early.store(true, Ordering::SeqCst);
    let scenario = scenario(
        json!([{"user":REFERENCE,"expect":[{"event":"llm_response","text_contains":"early assertion","within_ms":2000}]}]),
    );
    let now = tokio::time::Instant::now();
    let result = run(session(
        scenario,
        &host.url,
        settings(&http.url, Some(&root), Duration::from_secs(2)),
    ))
    .await;
    let elapsed = now.elapsed();
    let cache_settled_on_return = cache_wavs(&root).len();
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(result.passed, "{result:?}");
    assert!(
        elapsed >= Duration::from_millis(110),
        "run returned before fourth paced frame: {elapsed:?}"
    );
    assert_eq!(
        cache_settled_on_return, 1,
        "requested cache work outlived success"
    );
    assert_private_cache(&root, &bytes);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert!(report.is_ok());
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
    // This deliberately proves only local owner settlement. The complete-route
    // test above separately requires the post-consumption endpoint receipt.
}

#[tokio::test]
async fn concurrent_sessions_share_one_cold_cache_transaction_then_both_consume_full_pcm() {
    let b = Bundle::new();
    let root = b.cache();
    let bytes = pcm(93);
    let mut reply = Reply::pcm(bytes.clone());
    reply.delay = Duration::from_millis(150);
    let http = Http::start(vec![reply]).await;
    let host1 = Host::start(vec![bytes.clone()]).await;
    let host2 = Host::start(vec![bytes.clone()]).await;
    let configured = settings(&http.url, Some(&root), Duration::from_secs(3));
    let a = session(one(), &host1.url, configured.clone());
    let b = session(one(), &host2.url, configured);
    let (a, b) = tokio::join!(run(a), run(b));
    let (report1, trace1) = host1.finish().await;
    let (report2, trace2) = host2.finish().await;
    let http_trace = http.finish().await;
    assert!(a.passed && b.passed, "a={a:?}; b={b:?}");
    assert!(report1.is_ok() && report2.is_ok());
    assert_consumed(&trace1, std::slice::from_ref(&bytes));
    assert_consumed(&trace2, std::slice::from_ref(&bytes));
    assert_private_cache(&root, &bytes);
    assert_eq!(
        http_trace.requests.lock().unwrap().len(),
        1,
        "duplicate cold provider call"
    );
}

#[tokio::test]
async fn dtmf_and_observation_stay_synthesis_free_against_explicitly_compatible_endpoint() {
    let b = Bundle::new();
    let mut value = source();
    value["turns"] = json!([
        {"dtmf":"12#","expect":[{"event":"llm_response","text_contains":"DTMF accepted","within_ms":1000}]},
        {"expect":[{"event":"user_transcription","absent":true,"within_ms":40}]}
    ]);
    let path = b.json("dtmf.json", &value);
    let http = Http::start(vec![]).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = WebSocketSession::accept(stream, transport()).await?;
        let mut seen = Vec::new();
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else { continue };
            let message: Value = serde_json::from_str(&text)?;
            seen.push(message.clone());
            let events = match message["type"].as_str() {
                Some("client-ready") => vec![("bot-ready", json!({"version":"2.2.0"}))],
                Some("dtmf") => vec![
                    ("bot-llm-started", json!({})),
                    ("bot-llm-text", json!({"text":"DTMF accepted"})),
                    ("bot-llm-stopped", json!({})),
                ],
                Some("client-message") if message["data"]["t"] == "eval-cancel" => break,
                _ => vec![],
            };
            for (kind, data) in events {
                socket
                    .sender()
                    .send(Packet::Text(
                        json!({"label":"rtvi-ai","type":kind,"data":data}).to_string(),
                    ))
                    .await?;
            }
        }
        socket.finish().await?;
        Ok::<_, Error>(seen)
    });
    let mut sessions = UserSpeechScenarioFile::load(path)
        .unwrap()
        .into_sessions(
            &url,
            options(),
            settings(&http.url, None, Duration::from_secs(2)),
        )
        .unwrap();
    let result = run(sessions.remove(0)).await;
    let seen = peer.await.unwrap();
    let http_trace = http.finish().await;
    assert!(result.passed, "{result:?}");
    let seen = seen.unwrap();
    assert_eq!(seen.iter().filter(|m| m["type"] == "dtmf").count(), 1);
    assert!(seen.iter().all(|m| m["type"] != "send-text"));
    assert!(http_trace.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn remote_bot_interruption_does_not_cancel_admitted_user_speech() {
    let bytes = pcm(193);
    let mut reply = Reply::pcm(bytes.clone());
    reply.delay = Duration::from_millis(220);
    let http = Http::start(vec![reply]).await;
    let host = Host::start(vec![bytes.clone()]).await;
    let session = session(
        one(),
        &host.url,
        settings(&http.url, None, Duration::from_secs(2)),
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { session.run(c).await });
    let admitted = tokio::time::timeout(WAIT, http.trace.admitted.notified())
        .await
        .is_ok();
    host.trace.interrupt.store(true, Ordering::SeqCst);
    let result = joined(task, &cancel).await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(admitted);
    assert!(
        result.passed,
        "outgoing scripted speech was cancelled by bot interruption: {result:?}"
    );
    assert!(
        result
            .events_seen
            .iter()
            .any(|e| e["type"] == "bot_interrupted"),
        "fixture did not deliver actual interruption: {result:?}"
    );
    assert!(report.is_ok());
    assert_consumed(&trace, &[bytes]);
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn synthesis_begins_only_after_local_send_after_admission() {
    let bytes = pcm(203);
    let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let host = Host::start(vec![bytes.clone()]).await;
    let scenario =
        scenario(json!([{"user":REFERENCE,"send_after":{"delay_ms":250},"expect":[expected(1)]}]));
    let session = session(
        scenario,
        &host.url,
        settings(&http.url, None, Duration::from_secs(2)),
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { session.run(c).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let before_admission = http.trace.requests.lock().unwrap().len();
    let result = joined(task, &cancel).await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert_eq!(
        before_admission, 0,
        "scenario was synthesized before its turn was admitted"
    );
    assert!(result.passed, "{result:?}");
    assert!(report.is_ok());
    assert_consumed(&trace, &[bytes]);
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cache_identity_distinguishes_exact_case_voice_model_language_and_endpoint() {
    let b = Bundle::new();
    let root = b.cache();
    let bytes = pcm(213);
    let http = Http::start(vec![Reply::pcm(bytes.clone()); 5]).await;
    let other_http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let lower = REFERENCE.to_lowercase();
    let cases = [
        (REFERENCE, "Fixture-VOICE", None, None, http.url.as_str()),
        (
            lower.as_str(),
            "Fixture-VOICE",
            None,
            None,
            http.url.as_str(),
        ),
        (REFERENCE, "fixture-voice", None, None, http.url.as_str()),
        (
            REFERENCE,
            "Fixture-VOICE",
            Some("sonic-2-fixture"),
            None,
            http.url.as_str(),
        ),
        (
            REFERENCE,
            "Fixture-VOICE",
            None,
            Some("es"),
            http.url.as_str(),
        ),
        (
            REFERENCE,
            "Fixture-VOICE",
            None,
            None,
            other_http.url.as_str(),
        ),
    ];
    let mut results = Vec::new();
    for (text, voice_id, model, language, endpoint) in cases {
        let host = Host::start(vec![bytes.clone()]).await;
        let scenario = scenario(json!([{"user":text,"expect":[expected(1)]}]));
        let session = EvalSession::new(scenario, &host.url, options())
            .unwrap()
            .with_cartesia_user_speech(
                settings(endpoint, Some(&root), Duration::from_secs(2)),
                CartesiaUserSpeechVoice::new(
                    voice_id,
                    model.map(str::to_owned),
                    language.map(str::to_owned),
                )
                .unwrap(),
            )
            .unwrap();
        let result = run(session).await;
        let (report, trace) = host.finish().await;
        results.push((result, report, trace));
    }
    let endpoint = http.url.clone();
    let http_trace = http.finish().await;
    let other_trace = other_http.finish().await;
    // Explicit effective defaults must share the first key, even after the
    // original HTTP endpoint has closed.
    let host = Host::start(vec![bytes.clone()]).await;
    let session = EvalSession::new(one(), &host.url, options())
        .unwrap()
        .with_cartesia_user_speech(
            settings(&endpoint, Some(&root), Duration::from_secs(2)),
            CartesiaUserSpeechVoice::new(
                "Fixture-VOICE",
                Some("sonic-2".into()),
                Some("en".into()),
            )
            .unwrap(),
        )
        .unwrap();
    let result = run(session).await;
    let (report, trace) = host.finish().await;
    results.push((result, report, trace));
    for (result, report, trace) in results {
        assert!(result.passed, "{result:?}");
        assert!(report.is_ok());
        assert_consumed(&trace, std::slice::from_ref(&bytes));
    }
    let requests = http_trace.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].body["transcript"], REFERENCE);
    assert_eq!(requests[1].body["transcript"], lower);
    assert_eq!(requests[2].body["voice"]["id"], "fixture-voice");
    assert_eq!(requests[3].body["model_id"], "sonic-2-fixture");
    assert_eq!(requests[4].body["language"], "es");
    assert_eq!(other_trace.requests.lock().unwrap().len(), 1);
    assert_eq!(cache_wavs(&root).len(), 6);
}

#[tokio::test]
async fn cache_lock_wait_has_its_own_fixed_deadline_without_second_http_call() {
    let b = Bundle::new();
    let root = b.cache();
    let mut reply = Reply::pcm(vec![]);
    reply.stalled = true;
    let http = Http::start(vec![reply]).await;
    let host1 = Host::start(vec![pcm(223)]).await;
    let session1 = session(
        one(),
        &host1.url,
        settings(&http.url, Some(&root), Duration::from_secs(4)),
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let first = tokio::spawn(async move { session1.run(c).await });
    let admitted = tokio::time::timeout(WAIT, http.trace.admitted.notified())
        .await
        .is_ok();
    let host2 = Host::start(vec![pcm(223)]).await;
    host2.trace.chatty.store(true, Ordering::SeqCst);
    let second = run(session(
        one(),
        &host2.url,
        settings(&http.url, Some(&root), Duration::from_millis(180)),
    ))
    .await;
    cancel.cancel();
    let first = joined(first, &cancel).await;
    let (report1, trace1) = host1.finish().await;
    let (report2, trace2) = host2.finish().await;
    let http_trace = http.finish().await;
    assert!(admitted);
    assert!(
        second
            .failures
            .iter()
            .any(|f| f.kind == "user_speech_cache_timeout"),
        "cache timeout was swallowed or reclassified: {second:?}"
    );
    assert!(
        first.failures.iter().any(|f| f.kind == "cancelled"),
        "{first:?}"
    );
    assert!(report1.is_ok() && report2.is_ok());
    assert_no_speech(&trace1);
    assert_no_speech(&trace2);
    assert_eq!(
        http_trace.requests.lock().unwrap().len(),
        1,
        "lock waiter bypassed cache and called paid provider"
    );
    assert_no_cached_audio(&root);
}

#[tokio::test]
async fn stricter_explicit_upstream_audio_limit_is_preserved_before_playback() {
    let bytes = pcm(233);
    let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
    let host = Host::start(vec![bytes]).await;
    let mut config = SpeechConfig::cartesia(SECRET);
    config.base_url = http.url.clone();
    config.timeout = Duration::from_secs(2);
    config.max_audio_bytes = 1024;
    let configured =
        CartesiaUserSpeechSettings::new(config, UserSpeechCachePolicy::Disabled).unwrap();
    let result = run(session(one(), &host.url, configured)).await;
    let (report, trace) = host.finish().await;
    let http_trace = http.finish().await;
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.kind == "user_speech_error"),
        "{result:?}"
    );
    assert!(report.is_ok());
    assert_no_speech(&trace);
    assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
}
