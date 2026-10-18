//! Explicit scripted Cartesia speech with prevalidated recording overrides.
use crate::{
    recorded_audio::MAX_TOTAL_BYTES, scenario::ResponseParseMode, user_speech::UserSpeechBudget,
    CartesiaUserSpeechSettings, CartesiaUserSpeechVoice, EvalError, EvalSession,
    RecordedScenarioFile, SessionOptions,
};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// An opt-in, network-free scenario preflight. Ordinary scenario loaders keep
/// rejecting speech configuration. Recording paths retain trusted-local semantics.
#[derive(Debug)]
pub struct UserSpeechScenarioFile {
    file: RecordedScenarioFile,
    voices: Vec<Option<CartesiaUserSpeechVoice>>,
}
impl UserSpeechScenarioFile {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let mut voices = Vec::new();
        let mut user_turns = 0usize;
        let file = RecordedScenarioFile::load_with_transform(
            path.as_ref(),
            16 * 1024 * 1024,
            MAX_TOTAL_BYTES,
            128,
            ResponseParseMode::Text,
            |raw| {
                if voices.len() >= 16 {
                    return Err(EvalError::Limit("user speech scenarios"));
                }
                let (voice, recordings) = normalize(raw, &mut user_turns)?;
                voices.push(voice);
                Ok(recordings)
            },
        )?;
        Ok(Self { file, voices })
    }
    pub fn name(&self) -> &str {
        self.file.name()
    }
    pub fn len(&self) -> usize {
        self.file.len()
    }
    pub fn is_empty(&self) -> bool {
        self.file.is_empty()
    }
    /// Construct every session before connecting. The invocation budget charges
    /// actual preloaded recording bytes once, then actual synthesized PCM bytes.
    pub fn into_sessions(
        self,
        url: &str,
        options: SessionOptions,
        settings: CartesiaUserSpeechSettings,
    ) -> Result<Vec<EvalSession>, EvalError> {
        let parts = self.file.into_parts();
        let bytes =
            parts
                .iter()
                .flat_map(|(_, clips)| clips)
                .try_fold(0usize, |sum, (_, clip)| {
                    sum.checked_add(clip.pcm_bytes().len())
                        .ok_or(EvalError::Limit("user speech PCM budget"))
                })?;
        let budget = UserSpeechBudget::new(bytes)?;
        parts
            .into_iter()
            .zip(self.voices)
            .map(|((scenario, clips), voice)| {
                let session = EvalSession::new(scenario, url, options.clone())?
                    .with_recorded_audio_inputs(clips)?;
                match voice {
                    Some(voice) => Ok(session
                        .with_cartesia_user_speech(settings.clone(), voice)?
                        .with_user_speech_budget(budget.clone())),
                    None => Ok(session.with_user_speech_budget(budget.clone())),
                }
            })
            .collect()
    }
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
type NormalizedSpeech = (Option<CartesiaUserSpeechVoice>, Vec<(usize, PathBuf)>);

fn normalize(
    raw: &mut Map<String, Value>,
    count: &mut usize,
) -> Result<NormalizedSpeech, EvalError> {
    let user = raw
        .get_mut("user")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid("user speech requires user.modality: audio"))?;
    if user.get("modality").and_then(Value::as_str) != Some("audio") {
        return Err(invalid("user speech requires user.modality: audio"));
    }
    if user
        .keys()
        .any(|key| !matches!(key.as_str(), "modality" | "speech"))
    {
        return Err(invalid("unknown user speech configuration field"));
    }
    let voice = user
        .remove("speech")
        .filter(|v| !v.is_null())
        .map(parse_voice)
        .transpose()?;
    user.insert("modality".into(), "text".into());
    let turns = raw
        .get_mut("turns")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("turns must be a list"))?;
    let mut recordings = Vec::new();
    for (index, turn) in turns.iter_mut().enumerate() {
        let turn = turn
            .as_object_mut()
            .ok_or_else(|| invalid("turn must be a mapping"))?;
        let audio = turn.remove("audio").filter(|v| !v.is_null());
        let reference = turn.get("user").filter(|v| !v.is_null());
        if let Some(reference) = reference {
            let text = reference
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| invalid("audio input requires nonempty user reference text"))?;
            if text.len() > 16 * 1024 {
                return Err(EvalError::Limit("user speech text bytes"));
            }
            *count = count
                .checked_add(1)
                .ok_or(EvalError::Limit("user speech turns"))?;
            if *count > 128 {
                return Err(EvalError::Limit("user speech turns"));
            }
            if turn.get("dtmf").is_some_and(|v| !v.is_null()) {
                return Err(invalid("audio input and dtmf are mutually exclusive"));
            }
        }
        if let Some(audio) = audio {
            if reference.is_none() {
                return Err(invalid("recording requires user reference text"));
            }
            let path = audio
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| invalid("turn.audio must be a nonempty local WAV path"))?;
            if path.len() > 4096 {
                return Err(EvalError::Limit("recording path bytes"));
            }
            recordings.push((index, PathBuf::from(path)));
        } else if reference.is_some() && voice.is_none() {
            return Err(invalid("audio turn requires a recording or user.speech"));
        }
    }
    Ok((voice, recordings))
}
fn parse_voice(value: Value) -> Result<CartesiaUserSpeechVoice, EvalError> {
    let map = value
        .as_object()
        .ok_or_else(|| invalid("user.speech must be a mapping"))?;
    if map.keys().any(|key| {
        !matches!(
            key.as_str(),
            "service" | "voice" | "model" | "language" | "sample_rate" | "speed"
        )
    }) {
        return Err(invalid("unsupported user.speech configuration field"));
    }
    if !map
        .get("service")
        .and_then(Value::as_str)
        .is_some_and(|s| s.eq_ignore_ascii_case("cartesia"))
    {
        return Err(invalid("user.speech supports only cartesia"));
    }
    let voice = map
        .get("voice")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("user.speech requires a voice"))?;
    if map
        .get("sample_rate")
        .is_some_and(|v| v.as_u64() != Some(16000))
    {
        return Err(invalid("user speech sample_rate must be 16000"));
    }
    if map.get("speed").is_some_and(|v| v.as_f64() != Some(1.0)) {
        return Err(invalid("user speech supports only speed 1.0"));
    }
    let text = |key: &str| -> Result<Option<String>, EvalError> {
        match map.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            _ => Err(invalid("speech model/language must be strings or null")),
        }
    };
    CartesiaUserSpeechVoice::new(voice, text("model")?, text("language")?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn scenario() -> Map<String, Value> {
        json!({"user":{"modality":"audio","speech":{"service":"cartesia","voice":"voice"}},
            "turns":[{"user":"Literal CASE"},{"user":"recorded", "audio":"clip.wav"},{"dtmf":"12"},{"expect":[]}]}).as_object().unwrap().clone()
    }
    #[test]
    fn speech_recording_dtmf_and_observation_preserve_turn_indices() {
        let mut raw = scenario();
        let mut count = 0;
        let (voice, recordings) = normalize(&mut raw, &mut count).unwrap();
        assert!(voice.is_some());
        assert_eq!(count, 2);
        assert_eq!(recordings, vec![(1, PathBuf::from("clip.wav"))]);
        assert_eq!(raw["user"], json!({"modality":"text"}));
        assert_eq!(raw["turns"][0]["user"], "Literal CASE");
        assert!(raw["turns"][1].get("audio").is_none());
        assert_eq!(raw["turns"][2]["dtmf"], "12");
    }
    #[test]
    fn recording_only_needs_no_speech_but_uncovered_turn_never_falls_back() {
        let mut raw = scenario();
        raw["user"].as_object_mut().unwrap().remove("speech");
        assert!(normalize(&mut raw.clone(), &mut 0).is_err());
        raw["turns"][0]["audio"] = json!("first.wav");
        let (voice, recordings) = normalize(&mut raw, &mut 0).unwrap();
        assert!(voice.is_none());
        assert_eq!(recordings.len(), 2);
    }
    #[test]
    fn rejected_settings_and_incompatible_turns_cannot_select_a_destination() {
        for (key, value) in [
            ("api_key", json!("secret")),
            ("base_url", json!("http://example.invalid")),
            ("factory", json!("python.factory")),
            ("extra_headers", json!({})),
            ("speed", json!(1.1)),
            ("sample_rate", json!(24000)),
            ("sample_rate", Value::Null),
            ("model", json!(1)),
            ("language", json!([])),
        ] {
            let mut raw = scenario();
            raw["user"]["speech"][key] = value;
            assert!(normalize(&mut raw, &mut 0).is_err(), "{key}");
        }
        for turn in [
            json!({"audio":"x.wav"}),
            json!({"dtmf":"1","audio":"x.wav"}),
            json!({"user":"x","dtmf":"1"}),
            json!({"user":"  "}),
            json!({"user":"x","audio":false}),
        ] {
            let mut raw = scenario();
            raw["turns"] = json!([turn]);
            assert!(normalize(&mut raw, &mut 0).is_err());
        }
    }
    #[test]
    fn exact_text_and_turn_limits_are_preflight_bounds() {
        let mut raw = scenario();
        raw["turns"] = json!([{"user":"x".repeat(16*1024)}]);
        let mut count = 127;
        normalize(&mut raw, &mut count).unwrap();
        assert_eq!(count, 128);
        let mut raw = scenario();
        assert!(normalize(&mut raw, &mut 128).is_err());
        let mut raw = scenario();
        raw["turns"][0]["user"] = json!("x".repeat(16 * 1024 + 1));
        assert!(normalize(&mut raw, &mut 0).is_err());
    }
    #[test]
    fn source_language_nonstring_vectors_are_rejected() {
        let oracle: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/cartesia_user_speech_languages.json"
        ))
        .unwrap();
        for case in oracle["cases"].as_array().unwrap() {
            if !case["input"].is_string() && !case["input"].is_null() {
                let mut raw = scenario();
                raw["user"]["speech"]["language"] = case["input"].clone();
                assert!(normalize(&mut raw, &mut 0).is_err());
            }
        }
    }
    #[tokio::test]
    async fn documented_bundle_loads_and_freezes_every_recording_plan() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "pipecat-speech-doc-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        std::fs::create_dir(root.join("recordings")).unwrap();
        let wav = pipecat_audio::pcm_to_wav(&[1, 0, 2, 0], 16000, 1).unwrap();
        std::fs::write(root.join("recordings/second.wav"), wav).unwrap();
        let doc = include_str!("../USER_SPEECH.md");
        let yaml = doc
            .split("```yaml\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        let path = root.join("scenario.yaml");
        std::fs::write(&path, yaml).unwrap();
        let file = UserSpeechScenarioFile::load(&path).unwrap();
        assert_eq!(file.name(), "spoken-input");
        assert_eq!(file.len(), 1);
        let settings = CartesiaUserSpeechSettings::new(
            pipecat_speech::SpeechConfig::cartesia("synthetic-key"),
            crate::UserSpeechCachePolicy::Disabled,
        )
        .unwrap();
        let session = file
            .into_sessions(
                "ws://127.0.0.1:1",
                SessionOptions::default(),
                settings.clone(),
            )
            .unwrap()
            .pop()
            .unwrap();
        assert!(session.with_recorded_audio_inputs([]).is_err());
        let recorded = json!({"name":"all-recorded","user":{"modality":"audio"},"turns":[{"user":"reference","audio":"recordings/second.wav"}]});
        std::fs::write(&path, serde_json::to_vec(&recorded).unwrap()).unwrap();
        let session = UserSpeechScenarioFile::load(&path)
            .unwrap()
            .into_sessions("ws://127.0.0.1:1", SessionOptions::default(), settings)
            .unwrap()
            .pop()
            .unwrap();
        assert!(session.with_recorded_audio_inputs([]).is_err());
    }
}
