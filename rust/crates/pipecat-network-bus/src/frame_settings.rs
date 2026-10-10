//! Sparse source settings use omission for NotGiven and explicit JSON null for
//! clearing a setting. No live service reference crosses the network.
use super::extended::{no_local, parse, tagged};
use super::*;
use serde::{de::DeserializeOwned, Serialize};
const PREFIX: &str = "pipecat.services.settings.";
const COMPLETION: &str = "pipecat.turns.user_turn_completion_mixin.UserTurnCompletionConfig";
pub(super) fn supported_type(name: &str) -> bool {
    matches!(
        name.strip_prefix(PREFIX),
        Some("ServiceSettings" | "LLMSettings" | "TTSSettings" | "STTSettings")
    ) || name == COMPLETION
}
fn setting<T: DeserializeOwned>(
    f: &Map<String, Value>,
    key: &str,
) -> Result<Setting<T>, CodecError> {
    match f.get(key) {
        None => Ok(Setting::NotGiven),
        Some(Value::Null) => Ok(Setting::Null),
        Some(value) => serde_json::from_value(value.clone())
            .map(Setting::Value)
            .map_err(|_| invalid(key)),
    }
}
fn base(f: &Map<String, Value>) -> Result<ServiceSettings, CodecError> {
    Ok(ServiceSettings {
        model: setting(f, "model")?,
        extra: defaulted(f, "extra", Default::default())?,
    })
}
fn delta(f: &Map<String, Value>, name: &str) -> Result<Option<Map<String, Value>>, CodecError> {
    f.get("delta")
        .filter(|v| !v.is_null())
        .map(|v| tagged(v, &format!("{PREFIX}{name}")).cloned())
        .transpose()
}
fn frame<T>(
    f: &Map<String, Value>,
    delta: Option<T>,
) -> Result<ServiceUpdateSettingsFrame<T>, CodecError> {
    no_local(f, &["service"])?;
    Ok(ServiceUpdateSettingsFrame {
        settings: defaulted(f, "settings", Default::default())?,
        delta,
        service: None,
        reach_inactive_services: defaulted(f, "reach_inactive_services", false)?,
    })
}
fn completion_setting(
    f: &Map<String, Value>,
) -> Result<Setting<UserTurnCompletionConfig>, CodecError> {
    match f.get("user_turn_completion_config") {
        None => Ok(Setting::NotGiven),
        Some(Value::Null) => Ok(Setting::Null),
        Some(value) => Ok(Setting::Value(parse(
            tagged(value, COMPLETION)?,
            serde_json::to_value(UserTurnCompletionConfig::default())?,
        )?)),
    }
}
fn llm(f: &Map<String, Value>) -> Result<LlmSettings, CodecError> {
    Ok(LlmSettings {
        base: base(f)?,
        system_instruction: setting(f, "system_instruction")?,
        temperature: setting(f, "temperature")?,
        max_tokens: setting(f, "max_tokens")?,
        top_p: setting(f, "top_p")?,
        top_k: setting(f, "top_k")?,
        frequency_penalty: setting(f, "frequency_penalty")?,
        presence_penalty: setting(f, "presence_penalty")?,
        seed: setting(f, "seed")?,
        filter_incomplete_user_turns: setting(f, "filter_incomplete_user_turns")?,
        user_turn_completion_config: completion_setting(f)?,
    })
}
fn tts(f: &Map<String, Value>) -> Result<TtsSettings, CodecError> {
    Ok(TtsSettings {
        base: base(f)?,
        voice: setting(f, "voice")?,
        language: setting(f, "language")?,
    })
}
fn stt(f: &Map<String, Value>) -> Result<SttSettings, CodecError> {
    Ok(SttSettings {
        base: base(f)?,
        language: setting(f, "language")?,
    })
}
pub(super) fn decode(name: &str, f: &Map<String, Value>) -> Result<FrameData, CodecError> {
    Ok(match name {
        "ServiceUpdateSettingsFrame" => FrameData::ServiceUpdateSettings(Box::new(frame(
            f,
            delta(f, "ServiceSettings")?
                .as_ref()
                .map(base)
                .transpose()?,
        )?)),
        "LLMUpdateSettingsFrame" => FrameData::LlmUpdateSettings(Box::new(frame(
            f,
            delta(f, "LLMSettings")?.as_ref().map(llm).transpose()?,
        )?)),
        "TTSUpdateSettingsFrame" => FrameData::TtsUpdateSettings(Box::new(frame(
            f,
            delta(f, "TTSSettings")?.as_ref().map(tts).transpose()?,
        )?)),
        "STTUpdateSettingsFrame" => FrameData::SttUpdateSettings(Box::new(frame(
            f,
            delta(f, "STTSettings")?.as_ref().map(stt).transpose()?,
        )?)),
        _ => return Err(invalid("settings frame type")),
    })
}
fn put<T: Serialize + DeserializeOwned + PartialEq>(
    f: &mut Map<String, Value>,
    key: &str,
    value: &Setting<T>,
) -> Result<(), CodecError> {
    match value {
        Setting::NotGiven => {}
        Setting::Null => {
            f.insert(key.into(), Value::Null);
        }
        Setting::Value(value) => {
            let encoded = serde_json::to_value(value)?;
            let restored: T = serde_json::from_value(encoded.clone())
                .map_err(|_| invalid("nonfinite setting"))?;
            if &restored != value {
                return Err(invalid("nonfinite setting"));
            }
            f.insert(key.into(), encoded);
        }
    }
    Ok(())
}
fn encode_base(value: &ServiceSettings) -> Result<Map<String, Value>, CodecError> {
    let mut f = Map::new();
    put(&mut f, "model", &value.model)?;
    f.insert("extra".into(), serde_json::to_value(&value.extra)?);
    Ok(f)
}
fn encode_llm(value: &LlmSettings) -> Result<Map<String, Value>, CodecError> {
    let mut f = encode_base(&value.base)?;
    put(&mut f, "system_instruction", &value.system_instruction)?;
    put(&mut f, "temperature", &value.temperature)?;
    put(&mut f, "max_tokens", &value.max_tokens)?;
    put(&mut f, "top_p", &value.top_p)?;
    put(&mut f, "top_k", &value.top_k)?;
    put(&mut f, "frequency_penalty", &value.frequency_penalty)?;
    put(&mut f, "presence_penalty", &value.presence_penalty)?;
    put(&mut f, "seed", &value.seed)?;
    put(
        &mut f,
        "filter_incomplete_user_turns",
        &value.filter_incomplete_user_turns,
    )?;
    match &value.user_turn_completion_config {
        Setting::NotGiven => {}
        Setting::Null => {
            f.insert("user_turn_completion_config".into(), Value::Null);
        }
        Setting::Value(value) => {
            if !value.incomplete_short_timeout.is_finite()
                || !value.incomplete_long_timeout.is_finite()
            {
                return Err(invalid("nonfinite completion timeout"));
            }
            f.insert(
                "user_turn_completion_config".into(),
                tag(COMPLETION, super::extended::plain(value)?),
            );
        }
    }
    Ok(f)
}
fn encode_tts(value: &TtsSettings) -> Result<Map<String, Value>, CodecError> {
    let mut f = encode_base(&value.base)?;
    put(&mut f, "voice", &value.voice)?;
    put(&mut f, "language", &value.language)?;
    Ok(f)
}
fn encode_stt(value: &SttSettings) -> Result<Map<String, Value>, CodecError> {
    let mut f = encode_base(&value.base)?;
    put(&mut f, "language", &value.language)?;
    Ok(f)
}
fn encode_frame<T>(
    value: &ServiceUpdateSettingsFrame<T>,
    name: &str,
    convert: fn(&T) -> Result<Map<String, Value>, CodecError>,
) -> Result<Value, CodecError> {
    if value.service.is_some() {
        return Err(invalid("settings service reference is local-only"));
    }
    let mut f =
        json!({"settings":value.settings,"reach_inactive_services":value.reach_inactive_services});
    if let Some(delta) = &value.delta {
        f["delta"] = tag(&format!("{PREFIX}{name}"), convert(delta)?.into());
    }
    Ok(f)
}
pub(super) fn encode(data: &FrameData) -> Result<Value, CodecError> {
    match data {
        FrameData::ServiceUpdateSettings(v) => encode_frame(v, "ServiceSettings", encode_base),
        FrameData::LlmUpdateSettings(v) => encode_frame(v, "LLMSettings", encode_llm),
        FrameData::TtsUpdateSettings(v) => encode_frame(v, "TTSSettings", encode_tts),
        FrameData::SttUpdateSettings(v) => encode_frame(v, "STTSettings", encode_stt),
        _ => Err(invalid("settings frame type")),
    }
}
