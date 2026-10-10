use super::settings::GeminiImageLimits;
use crate::{ImageError, Result};
use serde::{
    de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize,
};
use std::fmt;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InlineImage {
    pub mime_type: Option<String>,
    pub data: Option<String>,
}
struct Scan<'a> {
    limits: &'a GeminiImageLimits,
    candidates: usize,
    parts: usize,
    first: Option<InlineImage>,
    too_large: bool,
    blocked: bool,
}
#[derive(Clone, Copy)]
enum Kind {
    Reply,
    Candidate,
    Content,
    Part,
}
struct Node<'a, 'b>(Kind, &'a mut Scan<'b>);
struct List<'a, 'b>(Kind, &'a mut Scan<'b>);
impl<'de> DeserializeSeed<'de> for Node<'_, '_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a Gemini response object")
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        if matches!(self.0, Kind::Content) {
            Ok(())
        } else {
            Err(E::custom("invalid response object"))
        }
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        // Duplicate interpreted keys are rejected rather than allowing a later
        // candidate/content field to bypass the count or first-image contract.
        let mut seen = 0u8;
        while let Some(key) = map.next_key::<String>()? {
            let bit = match (self.0, key.as_str()) {
                (Kind::Reply, "candidates")
                | (Kind::Candidate, "content")
                | (Kind::Content, "parts")
                | (Kind::Part, "inlineData") => 1,
                (Kind::Reply, "promptFeedback") | (Kind::Candidate, "finishReason") => 2,
                _ => 0,
            };
            if bit != 0 && seen & bit != 0 {
                return Err(de::Error::custom("duplicate response field"));
            }
            seen |= bit;
            match (self.0, key.as_str()) {
                (Kind::Reply, "candidates") => {
                    map.next_value_seed(List(Kind::Candidate, self.1))?
                }
                (Kind::Candidate, "content") => map.next_value_seed(Node(Kind::Content, self.1))?,
                (Kind::Content, "parts") => map.next_value_seed(List(Kind::Part, self.1))?,
                (Kind::Part, "inlineData") if self.1.first.is_none() => {
                    let image = map.next_value::<Option<InlineImage>>()?;
                    if image
                        .as_ref()
                        .is_some_and(|image| image.data.as_ref().is_some_and(|s| !s.is_empty()))
                    {
                        self.1.first = image;
                    }
                }
                (Kind::Reply, "promptFeedback") => {
                    #[derive(Deserialize)]
                    #[serde(rename_all = "camelCase")]
                    struct Feedback {
                        block_reason: Option<String>,
                    }
                    if let Some(feedback) = map.next_value::<Option<Feedback>>()? {
                        self.1.blocked |= feedback
                            .block_reason
                            .as_ref()
                            .is_some_and(|v| !v.is_empty() && v != "BLOCK_REASON_UNSPECIFIED");
                    }
                }
                (Kind::Candidate, "finishReason") => {
                    let reason = map.next_value::<Option<String>>()?;
                    self.1.blocked |= reason.as_deref().is_some_and(|r| {
                        matches!(
                            r,
                            "SAFETY"
                                | "RECITATION"
                                | "BLOCKLIST"
                                | "PROHIBITED_CONTENT"
                                | "SPII"
                                | "IMAGE_SAFETY"
                                | "IMAGE_PROHIBITED_CONTENT"
                                | "IMAGE_RECITATION"
                        )
                    });
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}
impl<'de> DeserializeSeed<'de> for List<'_, '_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for List<'_, '_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded Gemini response array")
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        loop {
            let (count, maximum) = match self.0 {
                Kind::Candidate => (self.1.candidates, self.1.limits.max_candidates),
                Kind::Part => (self.1.parts, self.1.limits.max_parts),
                _ => unreachable!(),
            };
            if count == maximum {
                if seq.next_element::<IgnoredAny>()?.is_some() {
                    self.1.too_large = true;
                    return Err(de::Error::custom("response entry limit"));
                }
                break;
            }
            match self.0 {
                Kind::Candidate => self.1.candidates += 1,
                Kind::Part => self.1.parts += 1,
                _ => unreachable!(),
            }
            if seq.next_element_seed(Node(self.0, self.1))?.is_none() {
                match self.0 {
                    Kind::Candidate => self.1.candidates -= 1,
                    Kind::Part => self.1.parts -= 1,
                    _ => unreachable!(),
                }
                break;
            }
        }
        Ok(())
    }
}
pub(super) fn first_image(body: &[u8], limits: &GeminiImageLimits) -> Result<InlineImage> {
    let mut scan = Scan {
        limits,
        candidates: 0,
        parts: 0,
        first: None,
        too_large: false,
        blocked: false,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    if Node(Kind::Reply, &mut scan)
        .deserialize(&mut deserializer)
        .is_err()
    {
        return Err(if scan.too_large {
            ImageError::TooLarge
        } else {
            ImageError::Protocol
        });
    }
    deserializer.end().map_err(|_| ImageError::Protocol)?;
    if scan.blocked {
        return Err(ImageError::NoImage);
    }
    scan.first.ok_or(ImageError::NoImage)
}
