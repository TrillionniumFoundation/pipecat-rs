//! One immutable source-shaped plan for user audio and spoken bot responses.
use crate::{
    native_loader::LoaderRegistry, scenario::ResponseParseMode, user_speech::UserSpeechBudget,
    user_speech_scenario::load_speech_plan, BotTranscriptionSettings, Cancellation,
    CartesiaUserSpeechSettings, CartesiaUserSpeechVoice, EvalError, EvalSession, RecordedAudio,
    ScriptScenario, SessionOptions,
};
use std::{path::Path, sync::Arc};

#[derive(Debug)]
struct PreparedScenario {
    scenario: ScriptScenario,
    recordings: Vec<(usize, RecordedAudio)>,
    voice: Option<CartesiaUserSpeechVoice>,
}

/// Explicit, bounded audio-to-audio evaluation plan. Loading preserves spoken
/// `response` independently of reported `llm_response`, validates the complete
/// include tree, and reads contained local WAV overrides without network/model
/// work. Fields are private so coverage and invocation precharges cannot change.
/// Available only with both `user-speech` and `bot-audio-transcription`.
#[derive(Debug)]
pub struct AudioToAudioScenarioFile {
    name: String,
    scenarios: Vec<PreparedScenario>,
    recorded_pcm_bytes: usize,
}

impl AudioToAudioScenarioFile {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let (file, voices) =
            load_speech_plan(path.as_ref(), ResponseParseMode::AudioResponse, true)?;
        let name = file.name().to_owned();
        let parts = file.into_parts();
        if parts.is_empty() || parts.len() > 16 || parts.len() != voices.len() {
            return Err(EvalError::Limit("audio-to-audio scenarios (1..16)"));
        }
        let mut recorded_pcm_bytes = 0usize;
        let mut scenarios = Vec::with_capacity(parts.len());
        for ((scenario, recordings), voice) in parts.into_iter().zip(voices) {
            for (_, clip) in &recordings {
                recorded_pcm_bytes = recorded_pcm_bytes
                    .checked_add(clip.pcm_bytes().len())
                    .ok_or(EvalError::Limit("user speech PCM budget"))?;
            }
            // A source voice is still validated when unused. An all-recording
            // scenario needs no synthesis owner, but remains explicitly combined.
            let needs_synthesis = scenario.turns.iter().enumerate().any(|(index, turn)| {
                turn.user.is_some() && !recordings.iter().any(|(recorded, _)| *recorded == index)
            });
            let voice = if needs_synthesis {
                Some(voice.ok_or_else(|| {
                    EvalError::Scenario("audio turn requires a recording or user.speech".into())
                })?)
            } else {
                None
            };
            scenarios.push(PreparedScenario {
                scenario,
                recordings,
                voice,
            });
        }
        // Validate the exact immutable precharge now; the eventual shared budget
        // charges these recordings once and every synthesized/cache-hit use.
        UserSpeechBudget::new(recorded_pcm_bytes)?;
        Ok(Self {
            name,
            scenarios,
            recorded_pcm_bytes,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn len(&self) -> usize {
        self.scenarios.len()
    }
    pub fn is_empty(&self) -> bool {
        self.scenarios.is_empty()
    }

    /// Validate every option, scenario, judge and recording before native model
    /// entry. Then prepare at most sixteen independent Silero configurations
    /// sequentially through owned loaders, before any session can connect.
    /// Only driving a session starts its PCM analysis thread. No turn is
    /// synthesized or read from the private cache during preparation.
    ///
    /// Cancellation stops further admission and returns `EvalError::Cancelled`
    /// after any entered native loader has actually joined. Native work has no
    /// hard preemption deadline; externally dropping this future synchronously
    /// joins an outstanding loader. Returned sessions retain frozen recordings
    /// and the shared invocation input budget, including all-WAV scenarios.
    pub async fn into_sessions_with_cancellation(
        self,
        url: &str,
        options: SessionOptions,
        speech_settings: CartesiaUserSpeechSettings,
        transcription_settings: BotTranscriptionSettings,
        cancel: Cancellation,
    ) -> Result<Vec<EvalSession>, EvalError> {
        if cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        transcription_settings.validate_audio_to_audio()?;
        options.validate_audio_to_audio()?;
        if options.stop_bot && self.scenarios.len() > 1 {
            return Err(EvalError::Scenario(
                "--stop-bot with several scenarios needs independent bot endpoints; use separate runs"
                    .into(),
            ));
        }
        for prepared in &self.scenarios {
            EvalSession::validate_audio_to_audio_plan(
                &prepared.scenario,
                url,
                &options,
                &prepared.recordings,
            )?;
        }
        let budget = UserSpeechBudget::new(self.recorded_pcm_bytes)?;
        let settings = Arc::new(transcription_settings);
        let loaders = LoaderRegistry::new();
        let registrar = loaders.registrar();
        let mut sessions = Vec::with_capacity(self.scenarios.len());
        for prepared in self.scenarios {
            let Some(config) = settings
                .clone()
                .build_config_owned(&cancel, &registrar)
                .await?
            else {
                return Err(EvalError::Cancelled);
            };
            if cancel.is_cancelled() {
                return Err(EvalError::Cancelled);
            }
            sessions.push(EvalSession::from_audio_to_audio_plan(
                prepared.scenario,
                url,
                options.clone(),
                config,
                prepared.voice.map(|voice| (speech_settings.clone(), voice)),
                prepared.recordings,
                budget.clone(),
            )?);
        }
        if cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        Ok(sessions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Mutex,
        },
        time::Duration,
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "pipecat-audio-plan-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn write(&self, value: Value) -> PathBuf {
            let path = self.0.join("scenarios.json");
            std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            path
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn source() -> Value {
        json!({
            "name": "duplex", "user": {"modality": "audio", "speech": {
                "service": "cartesia", "voice": "fixture-voice"
            }},
            "turns": [
                {"user": "reference", "expect": [
                    {"event": "response", "text_contains": "spoken"},
                    {"event": "llm_response", "text_contains": "reported"}
                ]},
                {"user": "next", "send_after": {"event": "response"}}
            ]
        })
    }
    fn speech() -> CartesiaUserSpeechSettings {
        let mut config = pipecat_speech::SpeechConfig::cartesia("synthetic-plan-key");
        config.base_url = "http://127.0.0.1:1".into();
        CartesiaUserSpeechSettings::new(config, crate::UserSpeechCachePolicy::Disabled).unwrap()
    }
    fn no_model(counter: Arc<AtomicUsize>) -> BotTranscriptionSettings {
        BotTranscriptionSettings::for_native_loader_test(
            || panic!("combined preparation does not use disposable suite preflight"),
            move || {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(EvalError::Scenario(
                    "unexpected synthetic model entry".into(),
                ))
            },
        )
    }

    #[test]
    fn spoken_parse_mode_is_selected_once_and_standalone_alias_is_unchanged() {
        let fixture = Fixture::new();
        let path = fixture.write(source());
        let plan = AudioToAudioScenarioFile::load(&path).unwrap();
        assert_eq!(plan.name(), "duplex");
        assert_eq!(plan.len(), 1);
        assert!(!plan.is_empty());
        let scenario = &plan.scenarios[0].scenario;
        assert_eq!(scenario.turns[0].expect[0].event, "response");
        assert_eq!(scenario.turns[0].expect[1].event, "llm_response");
        assert_eq!(
            scenario.turns[1]
                .send_after
                .as_ref()
                .unwrap()
                .event
                .as_deref(),
            Some("response")
        );
        // The existing Text parser aliases expectations only; it has always
        // rejected spoken send_after.response rather than aliasing that field.
        assert!(matches!(
            load_speech_plan(&path, ResponseParseMode::Text, false),
            Err(EvalError::Unsupported(message))
                if message == "event \"response\" (native session supports text mode only)"
        ));
        let mut legacy = source();
        legacy["turns"][1]["send_after"]["event"] = "llm_response".into();
        let legacy_path = fixture.write(legacy);
        let (text, _) = load_speech_plan(&legacy_path, ResponseParseMode::Text, false).unwrap();
        let text = text.into_parts().pop().unwrap().0;
        assert_eq!(text.turns[0].expect[0].event, "llm_response");
        assert_eq!(
            text.turns[1].send_after.as_ref().unwrap().event.as_deref(),
            Some("llm_response")
        );
    }

    #[test]
    fn later_invalid_scenario_is_rejected_before_recording_payload_access() {
        let fixture = Fixture::new();
        let value = json!({"name":"bundle", "user":{"modality":"audio"}, "scenarios":[
            {"name":"first", "turns":[{"user":"recorded", "audio":"missing.wav"}]},
            {"name":"second", "turns":[{"dtmf":"1"}]}
        ]});
        let error = AudioToAudioScenarioFile::load(fixture.write(value)).unwrap_err();
        assert!(matches!(error, EvalError::Unsupported(message) if message.contains("DTMF")));
    }

    #[test]
    fn all_wav_precharge_is_exact_and_does_not_retain_unused_synthesis() {
        let fixture = Fixture::new();
        let pcm = [1, 0, 2, 0, 3, 0];
        std::fs::write(
            fixture.0.join("clip.wav"),
            pipecat_audio::pcm_to_wav(&pcm, 16_000, 1).unwrap(),
        )
        .unwrap();
        let mut value = source();
        for turn in value["turns"].as_array_mut().unwrap() {
            turn["audio"] = "clip.wav".into();
        }
        let plan = AudioToAudioScenarioFile::load(fixture.write(value)).unwrap();
        assert_eq!(plan.recorded_pcm_bytes, 2 * pcm.len());
        assert_eq!(plan.scenarios[0].recordings.len(), 2);
        assert!(plan.scenarios[0].voice.is_none());
    }

    #[test]
    fn sixteen_scenarios_are_admitted_and_seventeen_are_rejected() {
        let fixture = Fixture::new();
        let mut value = source();
        value.as_object_mut().unwrap().remove("turns");
        value["scenarios"] = json!((0..16)
            .map(|i| json!({"name":format!("s{i}"), "turns":[{"user":"reference"}]}))
            .collect::<Vec<_>>());
        assert_eq!(
            AudioToAudioScenarioFile::load(fixture.write(value.clone()))
                .unwrap()
                .len(),
            16
        );
        value["scenarios"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"s16", "turns":[{"user":"reference"}]}));
        assert!(matches!(
            AudioToAudioScenarioFile::load(fixture.write(value)),
            Err(EvalError::Limit(_))
        ));
    }

    #[tokio::test]
    async fn cancellation_url_and_option_failures_enter_no_model() {
        let fixture = Fixture::new();
        let path = fixture.write(source());
        let attempts = Arc::new(AtomicUsize::new(0));
        let cancel = Cancellation::new();
        cancel.cancel();
        let result = AudioToAudioScenarioFile::load(&path)
            .unwrap()
            .into_sessions_with_cancellation(
                "ws://127.0.0.1:1",
                SessionOptions::default(),
                speech(),
                no_model(attempts.clone()),
                cancel,
            )
            .await;
        assert!(matches!(result, Err(EvalError::Cancelled)));
        for (url, options) in [
            ("invalid-url", SessionOptions::default()),
            (
                "ws://127.0.0.1:1",
                SessionOptions {
                    max_wire_messages: 65_537,
                    ..Default::default()
                },
            ),
        ] {
            assert!(AudioToAudioScenarioFile::load(&path)
                .unwrap()
                .into_sessions_with_cancellation(
                    url,
                    options,
                    speech(),
                    no_model(attempts.clone()),
                    Cancellation::new()
                )
                .await
                .is_err());
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn later_missing_judge_and_multiple_stop_bot_enter_no_model() {
        let fixture = Fixture::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut value = source();
        value.as_object_mut().unwrap().remove("turns");
        value["scenarios"] = json!([
            {"name":"first", "turns":[{"user":"ordinary"}]},
            {"name":"second", "turns":[{"user":"semantic", "expect":[{"event":"response", "eval":"answers the question"}]}]}
        ]);
        let path = fixture.write(value);
        for stop_bot in [false, true] {
            let result = AudioToAudioScenarioFile::load(&path)
                .unwrap()
                .into_sessions_with_cancellation(
                    "ws://127.0.0.1:1",
                    SessionOptions {
                        stop_bot,
                        ..Default::default()
                    },
                    speech(),
                    no_model(attempts.clone()),
                    Cancellation::new(),
                )
                .await;
            assert!(matches!(result, Err(EvalError::Scenario(_))));
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preparation_cancellation_waits_for_entered_owned_loader() {
        let fixture = Fixture::new();
        let path = fixture.write(source());
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let finished = Arc::new(AtomicBool::new(false));
        let worker_finished = finished.clone();
        let settings = BotTranscriptionSettings::for_native_loader_test(
            || unreachable!(),
            move || {
                let _ = entered.lock().unwrap().take().unwrap().send(());
                release_rx.lock().unwrap().recv().unwrap();
                worker_finished.store(true, Ordering::SeqCst);
                Err(EvalError::Scenario("synthetic native failure".into()))
            },
        );
        let (progress, progress_rx) = mpsc::channel();
        // Release even if a regression blocks the current-thread executor.
        let watchdog = std::thread::spawn(move || {
            let progressed = progress_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            let _ = release.send(());
            progressed
        });
        let cancel = Cancellation::new();
        let mut pending = Box::pin(
            AudioToAudioScenarioFile::load(&path)
                .unwrap()
                .into_sessions_with_cancellation(
                    "ws://127.0.0.1:1",
                    SessionOptions::default(),
                    speech(),
                    settings,
                    cancel.clone(),
                ),
        );
        let early = tokio::select! {
            result = &mut pending => Some(result),
            _ = async {
                entered_rx.await.unwrap();
                cancel.cancel();
                tokio::time::sleep(Duration::from_millis(5)).await;
            } => None,
        };
        let returned_early = early.is_some();
        let finished_early = finished.load(Ordering::SeqCst);
        let _ = progress.send(());
        let result = match early {
            Some(result) => result,
            None => pending.await,
        };
        let progressed = watchdog.join().unwrap();
        assert!(progressed);
        assert!(!returned_early);
        assert!(!finished_early);
        assert!(finished.load(Ordering::SeqCst));
        assert!(matches!(result, Err(EvalError::Cancelled)));
    }
}
