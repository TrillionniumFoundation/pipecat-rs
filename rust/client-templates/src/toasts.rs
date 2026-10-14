//! Bounded plain-text projection of the pinned RTVI `ui-command` / `toast` model.
//! URLs are validated for type/size, then discarded; no navigation, images or actions.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use serde::Deserialize;
use serde_json::Value;

pub const MAX_TOASTS: usize = 4;
pub const MAX_TITLE_BYTES: usize = 512;
pub const MAX_SUBTITLE_BYTES: usize = 1024;
pub const MAX_DESCRIPTION_BYTES: usize = 4096;
pub const MAX_IMAGE_URL_BYTES: usize = 2048;
pub const MAX_EVENT_BYTES: usize = 8 * 1024;
pub const DEFAULT_DURATION_MS: u64 = 5000;
pub const MAX_DURATION_MS: u64 = 30000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Toast {
    pub id: u64,
    pub title: String,
    pub subtitle: Option<String>,
    pub description: Option<String>,
    /// True when a non-null image_url was supplied, including an empty string.
    pub image_omitted: bool,
    pub expires_at_ms: u64,
}

// Unknown fields are deliberately ignored, matching the source Pydantic default.
#[derive(Deserialize)]
struct Payload {
    title: String,
    subtitle: Option<String>,
    description: Option<String>,
    image_url: Option<String>,
    duration_ms: Option<u64>,
}

#[derive(Default, Debug)]
pub struct Toasts {
    epoch: Option<u64>,
    entries: Vec<Toast>,
    last_id: u64,
    last_now_ms: u64,
}
impl Toasts {
    /// Begin a fresh session. Callback identity is the pair (epoch, toast id).
    pub fn begin(&mut self, epoch: u64) {
        *self = Self {
            epoch: Some(epoch),
            ..Self::default()
        };
    }
    pub fn close(&mut self) {
        *self = Self::default();
    }
    pub fn entries(&self) -> &[Toast] {
        &self.entries
    }
    /// Backwards caller times clamp to the last accepted time. Invalid events,
    /// other commands, closed sessions and stale epochs never advance the clock.
    /// All fallible validation precedes expiry, eviction and id allocation.
    pub fn apply(&mut self, epoch: u64, now_ms: u64, message: &Value) -> Result<bool, String> {
        if self.epoch != Some(epoch)
            || message.get("label").and_then(Value::as_str) != Some("rtvi-ai")
            || message.get("type").and_then(Value::as_str) != Some("ui-command")
            || message.pointer("/data/command").and_then(Value::as_str) != Some("toast")
        {
            return Ok(false);
        }
        if serde_json::to_vec(message)
            .map_err(|e| e.to_string())?
            .len()
            > MAX_EVENT_BYTES
        {
            return Err("toast event exceeds serialized byte limit".into());
        }
        let payload: Payload = serde_json::from_value(
            message
                .pointer("/data/payload")
                .cloned()
                .unwrap_or(Value::Null),
        )
        .map_err(|e| format!("invalid toast payload: {e}"))?;
        for (field, text, limit) in [
            ("title", Some(payload.title.as_str()), MAX_TITLE_BYTES),
            ("subtitle", payload.subtitle.as_deref(), MAX_SUBTITLE_BYTES),
            (
                "description",
                payload.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            ),
            (
                "image_url",
                payload.image_url.as_deref(),
                MAX_IMAGE_URL_BYTES,
            ),
        ] {
            if text.is_some_and(|text| text.len() > limit) {
                return Err(format!("toast {field} exceeds UTF-8 byte limit"));
            }
        }
        let duration = payload.duration_ms.unwrap_or(DEFAULT_DURATION_MS);
        if !(1..=MAX_DURATION_MS).contains(&duration) {
            return Err("toast duration_ms must be an integer in 1..=30000".into());
        }
        let now = now_ms.max(self.last_now_ms);
        let deadline = now.checked_add(duration).ok_or("toast deadline overflow")?;
        let id = self.last_id.checked_add(1).ok_or("toast id overflow")?;
        self.entries.retain(|entry| entry.expires_at_ms > now);
        if self.entries.len() == MAX_TOASTS {
            self.entries.remove(0);
        }
        self.entries.push(Toast {
            id,
            title: payload.title,
            subtitle: payload.subtitle,
            description: payload.description,
            image_omitted: payload.image_url.is_some(),
            expires_at_ms: deadline,
        });
        self.last_id = id;
        self.last_now_ms = now;
        Ok(true)
    }
    /// Expiration is inclusive at the deadline; backwards times are clamped.
    pub fn expire(&mut self, epoch: u64, now_ms: u64) -> bool {
        if self.epoch != Some(epoch) {
            return false;
        }
        let now = now_ms.max(self.last_now_ms);
        self.last_now_ms = now;
        let before = self.entries.len();
        self.entries.retain(|entry| entry.expires_at_ms > now);
        before != self.entries.len()
    }
    pub fn dismiss(&mut self, epoch: u64, id: u64) -> bool {
        if self.epoch != Some(epoch) {
            return false;
        }
        let before = self.entries.len();
        self.entries.retain(|entry| entry.id != id);
        before != self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn id_overflow_is_atomic_and_last_id_is_usable() {
        let mut state = Toasts::default();
        state.begin(9);
        state.last_id = u64::MAX - 1;
        let event = json!({"label":"rtvi-ai","type":"ui-command","data":{"command":"toast","payload":{"title":"last"}}});
        assert!(state.apply(9, 0, &event).unwrap());
        assert_eq!(state.entries()[0].id, u64::MAX);
        let prior = state.entries.clone();
        assert!(state.apply(9, 6000, &event).is_err());
        assert_eq!(state.entries, prior);
        assert_eq!(state.last_now_ms, 0);
        assert_eq!(state.last_id, u64::MAX);
    }
}
