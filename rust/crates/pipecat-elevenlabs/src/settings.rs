//! Atomic, typed settings deltas; credentials and connection format stay fixed.
use crate::{ElevenLabsSttClient, ElevenLabsTtsClient, StreamingError};
use pipecat_core::{
    FrameProcessor, ProcessorReference, ServiceSettings, Setting, SttSettings, TtsSettings,
};
use serde_json::Value;
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, StreamingError>;

pub(crate) fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(name)) => name == processor.name(),
        Some(ProcessorReference::Local(target)) => std::ptr::addr_eq(target.0.as_ref(), processor),
    }
}
fn invalid() -> StreamingError {
    StreamingError::Invalid("unsupported setting or invalid setting value")
}
fn required(target: &mut String, value: &Setting<String>) -> Result<()> {
    match value {
        Setting::NotGiven => {}
        Setting::Value(value) if !value.is_empty() => *target = value.clone(),
        _ => return Err(invalid()),
    }
    Ok(())
}
fn optional(target: &mut Option<String>, value: &Setting<String>) -> Result<()> {
    match value {
        Setting::NotGiven => {}
        Setting::Null => *target = None,
        Setting::Value(value) if !value.is_empty() => *target = Some(value.clone()),
        _ => return Err(invalid()),
    }
    Ok(())
}
fn string(value: &Value) -> Result<Setting<String>> {
    match value {
        Value::Null => Ok(Setting::Null),
        Value::String(value) => Ok(Setting::Value(value.clone())),
        _ => Err(invalid()),
    }
}
pub(crate) fn tts_mapping(values: &BTreeMap<String, Value>) -> Result<TtsSettings> {
    let mut settings = TtsSettings::default();
    for (key, value) in values {
        match key.as_str() {
            "model" => settings.base.model = string(value)?,
            "voice" => settings.voice = string(value)?,
            "language" => settings.language = string(value)?,
            _ => {
                settings.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(settings)
}
pub(crate) fn stt_mapping(values: &BTreeMap<String, Value>) -> Result<SttSettings> {
    let mut settings = SttSettings::default();
    for (key, value) in values {
        match key.as_str() {
            "model" => settings.base.model = string(value)?,
            "language" => settings.language = string(value)?,
            _ => {
                settings.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(settings)
}
/// Fields promoted from extra must not silently override explicit typed fields.
fn promoted(
    base: &ServiceSettings,
    language: &Setting<String>,
    voice: Option<&Setting<String>>,
) -> Result<BTreeMap<String, Value>> {
    let mut values = base.extra.clone();
    for (key, field) in [("model", &base.model), ("language", language)] {
        match field {
            Setting::NotGiven => {}
            Setting::Null => {
                values.insert(key.into(), Value::Null);
            }
            Setting::Value(value) => {
                values.insert(key.into(), value.clone().into());
            }
        }
    }
    if let Some(field) = voice {
        match field {
            Setting::NotGiven => {}
            Setting::Null => {
                values.insert("voice".into(), Value::Null);
            }
            Setting::Value(value) => {
                values.insert("voice".into(), value.clone().into());
            }
        }
    }
    Ok(values)
}
pub(crate) fn elevenlabs(
    client: &ElevenLabsTtsClient,
    delta: &TtsSettings,
) -> Result<ElevenLabsTtsClient> {
    let mut config = client.config().clone();
    for (key, value) in promoted(&delta.base, &delta.language, Some(&delta.voice))? {
        match key.as_str() {
            "model" => required(&mut config.model, &string(&value)?)?,
            "voice" => required(&mut config.voice_id, &string(&value)?)?,
            "language" => optional(&mut config.language, &string(&value)?)?,
            "stability" => config.voice_settings.stability = number(&value)?,
            "similarity_boost" => config.voice_settings.similarity_boost = number(&value)?,
            "style" => config.voice_settings.style = number(&value)?,
            "speed" => config.voice_settings.speed = number(&value)?,
            "use_speaker_boost" => config.voice_settings.use_speaker_boost = boolean(&value)?,
            "apply_text_normalization" => {
                optional(&mut config.apply_text_normalization, &string(&value)?)?
            }
            _ => return Err(invalid()),
        }
    }
    ElevenLabsTtsClient::new(config)
}
pub(crate) fn stt(
    client: &ElevenLabsSttClient,
    delta: &SttSettings,
) -> Result<ElevenLabsSttClient> {
    let mut config = client.config().clone();
    for (key, value) in promoted(&delta.base, &delta.language, None)? {
        match key.as_str() {
            "model" => required(&mut config.model, &string(&value)?)?,
            "language" => optional(&mut config.language, &string(&value)?)?,
            "keyterms" => {
                config.keyterms = if value.is_null() {
                    vec![]
                } else {
                    value
                        .as_array()
                        .ok_or_else(invalid)?
                        .iter()
                        .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
                        .collect::<Result<_>>()?
                }
            }
            "no_verbatim" => config.no_verbatim = boolean(&value)?,
            "filter_background_audio" => config.filter_background_audio = boolean(&value)?,
            "vad_threshold" => config.vad_threshold = number(&value)?,
            "vad_silence_threshold_secs" => config.vad_silence_threshold_secs = number(&value)?,
            "min_speech_duration_ms" => config.min_speech_duration_ms = integer(&value)?,
            "min_silence_duration_ms" => config.min_silence_duration_ms = integer(&value)?,
            _ => return Err(invalid()),
        }
    }
    ElevenLabsSttClient::new(config)
}
fn number(value: &Value) -> Result<Option<f64>> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(
            value
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(invalid)?,
        ))
    }
}
fn boolean(value: &Value) -> Result<Option<bool>> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value.as_bool().ok_or_else(invalid)?))
    }
}
fn integer(value: &Value) -> Result<Option<u32>> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(
            u32::try_from(value.as_u64().ok_or_else(invalid)?).map_err(|_| invalid())?,
        ))
    }
}

/// Pinned source ISO-639 mapping; regional suffixes use their base language.
pub(crate) fn stt_language(language: &str) -> Result<String> {
    let base = language
        .split(['-', '_'])
        .next()
        .unwrap_or("")
        .to_lowercase();
    let code = match base.as_str() {
        "af" => "afr",
        "am" => "amh",
        "ar" => "ara",
        "hy" => "hye",
        "as" => "asm",
        "ast" => "ast",
        "az" => "aze",
        "be" => "bel",
        "bn" => "ben",
        "bs" => "bos",
        "bg" => "bul",
        "my" => "mya",
        "yue" => "yue",
        "ca" => "cat",
        "ceb" => "ceb",
        "ny" => "nya",
        "hr" => "hrv",
        "cs" => "ces",
        "da" => "dan",
        "nl" => "nld",
        "en" => "eng",
        "et" => "est",
        "fil" => "fil",
        "fi" => "fin",
        "fr" => "fra",
        "ff" => "ful",
        "gl" => "glg",
        "lg" => "lug",
        "ka" => "kat",
        "de" => "deu",
        "el" => "ell",
        "gu" => "guj",
        "ha" => "hau",
        "he" => "heb",
        "hi" => "hin",
        "hu" => "hun",
        "is" => "isl",
        "ig" => "ibo",
        "id" => "ind",
        "ga" => "gle",
        "it" => "ita",
        "ja" => "jpn",
        "jv" => "jav",
        "kea" => "kea",
        "kn" => "kan",
        "kk" => "kaz",
        "km" => "khm",
        "ko" => "kor",
        "ku" => "kur",
        "ky" => "kir",
        "lo" => "lao",
        "lv" => "lav",
        "ln" => "lin",
        "lt" => "lit",
        "luo" => "luo",
        "lb" => "ltz",
        "mk" => "mkd",
        "ms" => "msa",
        "ml" => "mal",
        "mt" => "mlt",
        "zh" => "zho",
        "mi" => "mri",
        "mr" => "mar",
        "mn" => "mon",
        "ne" => "nep",
        "nso" => "nso",
        "no" => "nor",
        "oc" => "oci",
        "or" => "ori",
        "ps" => "pus",
        "fa" => "fas",
        "pl" => "pol",
        "pt" => "por",
        "pa" => "pan",
        "ro" => "ron",
        "ru" => "rus",
        "sr" => "srp",
        "sn" => "sna",
        "sd" => "snd",
        "sk" => "slk",
        "sl" => "slv",
        "so" => "som",
        "es" => "spa",
        "sw" => "swa",
        "sv" => "swe",
        "ta" => "tam",
        "tg" => "tgk",
        "te" => "tel",
        "th" => "tha",
        "tr" => "tur",
        "uk" => "ukr",
        "umb" => "umb",
        "ur" => "urd",
        "uz" => "uzb",
        "vi" => "vie",
        "cy" => "cym",
        "wo" => "wol",
        "xh" => "xho",
        "zu" => "zul",
        _ if base.len() == 3 && base.bytes().all(|c| c.is_ascii_lowercase()) => return Ok(base),
        _ => return Err(invalid()),
    };
    Ok(code.into())
}

pub(crate) fn tts_language_supported(model: &str, language: &str) -> bool {
    let supported: &[&str] = match model {
        "eleven_flash_v2_5" | "eleven_turbo_v2_5" => &[
            "ar", "bg", "cs", "da", "de", "el", "en", "es", "fi", "fil", "fr", "hi", "hr", "hu",
            "id", "it", "ja", "ko", "ms", "nl", "no", "pl", "pt", "ro", "ru", "sk", "sv", "ta",
            "tr", "uk", "vi", "zh",
        ],
        "eleven_v3" | "eleven_v3_conversational" => &[
            "af", "ar", "as", "az", "be", "bg", "bn", "bs", "ca", "ceb", "cs", "cy", "da", "de",
            "el", "en", "es", "et", "fa", "fi", "fil", "fr", "ga", "gl", "gu", "ha", "he", "hi",
            "hr", "hu", "hy", "id", "is", "it", "ja", "jv", "ka", "kk", "kn", "ko", "ky", "lb",
            "ln", "lt", "lv", "mk", "ml", "mr", "ms", "ne", "nl", "no", "ny", "pa", "pl", "ps",
            "pt", "ro", "ru", "sd", "sk", "sl", "so", "sr", "sv", "sw", "ta", "te", "th", "tr",
            "uk", "ur", "vi", "zh",
        ],
        "eleven_v4_turbo" => &[
            "af", "ar", "as", "az", "be", "bg", "bn", "bs", "ca", "ceb", "cs", "cy", "da", "de",
            "el", "en", "es", "et", "fa", "fi", "fil", "fr", "ga", "gl", "gu", "ha", "he", "hi",
            "hr", "hu", "hy", "id", "is", "it", "ja", "jv", "ka", "kk", "kn", "ko", "ky", "lb",
            "ln", "lt", "lv", "mk", "ml", "mr", "ms", "ne", "nl", "no", "ny", "pa", "pl", "ps",
            "pt", "ro", "ru", "sd", "sk", "sl", "so", "sr", "sv", "sw", "ta", "te", "th", "tr",
            "uk", "ur", "vi", "zh", "ast", "mi", "mn", "mt", "my", "oc", "or", "tg", "uz", "yue",
        ],
        _ => &[],
    };
    supported.contains(&language)
}
