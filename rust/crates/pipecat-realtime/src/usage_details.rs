//! Private, same-event usage metadata. Public wire types remain unchanged.
use crate::{Event, RealtimeError};
use serde::{
    de::{IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{json, Value};
use std::{fmt, marker::PhantomData};

#[derive(Debug, Clone, Copy)]
pub(crate) struct UsageDetails {
    cache_read: Option<u64>,
    input_audio: Option<u64>,
    output_audio: Option<u64>,
    cached_audio: Option<u64>,
}
// These ancestors are read only as objects. The legacy public typed decoder
// also accepts positional sequences; skip those here instead of interpreting
// their first elements as unrelated fields or narrowing public admission.
fn object_field<'de, D, T>(deserializer: D, field: &'static str) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Field<T> {
        name: &'static str,
        marker: PhantomData<T>,
    }
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Field<T> {
        type Value = Option<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an object or legacy positional sequence")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut value = None;
            let mut seen = false;
            while let Some(key) = map.next_key::<String>()? {
                if key == self.name {
                    if seen {
                        return Err(serde::de::Error::duplicate_field(self.name));
                    }
                    seen = true;
                    value = map.next_value::<Option<T>>()?;
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(value)
        }
        fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<Self::Value, S::Error> {
            while sequence.next_element::<IgnoredAny>()?.is_some() {}
            Ok(None)
        }
    }
    deserializer.deserialize_any(Field {
        name: field,
        marker: PhantomData,
    })
}
struct Envelope {
    response: Option<UsageResponse>,
}
impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self {
            response: object_field(d, "response")?,
        })
    }
}
struct UsageResponse {
    usage: Option<Usage>,
}
impl<'de> Deserialize<'de> for UsageResponse {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self {
            usage: object_field(d, "usage")?,
        })
    }
}
struct Usage {
    input_token_details: Option<InputDetails>,
}
impl<'de> Deserialize<'de> for Usage {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self {
            input_token_details: object_field(d, "input_token_details")?,
        })
    }
}
struct InputDetails {
    cached_tokens_details: Option<CachedDetails>,
}
impl<'de> Deserialize<'de> for InputDetails {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self {
            cached_tokens_details: object_field(d, "cached_tokens_details")?,
        })
    }
}
struct CachedDetails {
    audio_tokens: Option<u64>,
}
impl<'de> Deserialize<'de> for CachedDetails {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectOnly;
        impl<'de> Visitor<'de> for ObjectOnly {
            type Value = CachedDetails;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a cached token details object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut audio_tokens = Some(0);
                let mut seen = false;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "audio_tokens" {
                        if seen {
                            return Err(serde::de::Error::duplicate_field("audio_tokens"));
                        }
                        seen = true;
                        audio_tokens = map.next_value::<Option<u64>>()?;
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(CachedDetails { audio_tokens })
            }
        }
        // Unlike derive(Deserialize), this has no sequence visitor: [] is not {}.
        deserializer.deserialize_map(ObjectOnly)
    }
}
impl UsageDetails {
    pub(crate) fn parse(text: &str, event: &Event) -> Result<Option<Self>, RealtimeError> {
        let Event::ResponseDone { response } = event else {
            return Ok(None);
        };
        let Some(usage) = response.usage.as_ref() else {
            return Ok(None);
        };
        // Typed event validation has already succeeded. Validate the newly
        // supported nested object before any actor state transition occurs.
        let raw: Envelope = serde_json::from_str(text)
            .map_err(|_| RealtimeError::Protocol("invalid cached token details"))?;
        let cached_audio = raw
            .response
            .and_then(|response| response.usage)
            .and_then(|usage| usage.input_token_details)
            .and_then(|details| details.cached_tokens_details)
            .and_then(|details| details.audio_tokens);
        let details = Self {
            cache_read: usage.input_token_details.as_ref().map(|d| d.cached_tokens),
            input_audio: usage.input_token_details.as_ref().map(|d| d.audio_tokens),
            output_audio: usage.output_token_details.as_ref().map(|d| d.audio_tokens),
            cached_audio,
        };
        Ok(Some(details))
    }
    pub(crate) fn metadata(&self, prompt: u64, completion: u64, total: u64) -> Option<Value> {
        let mut value =
            json!({"prompt_tokens":prompt,"completion_tokens":completion,"total_tokens":total});
        let mut any = false;
        for (key, count) in [
            ("cache_read_input_tokens", self.cache_read),
            ("input_audio_tokens", self.input_audio),
            ("output_audio_tokens", self.output_audio),
            ("cache_read_input_audio_tokens", self.cached_audio),
        ] {
            if let Some(count) = count {
                value[key] = json!(count);
                any = true;
            }
        }
        any.then_some(value)
    }
}
