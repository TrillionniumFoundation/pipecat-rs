//! Bounded RTVI text requests shared by the actual browser adapters and native tests.
//! Preparing or locally queueing a request is not server acceptance or playback.
use crate::{protocol, Transport};
use serde_json::{json, Value};

pub const MAX_TEXT_BYTES: usize = 16 * 1024;
pub const MAX_TEXT_WIRE_BYTES: usize = 128 * 1024;
pub const MAX_WEBRTC_TEXT_WIRE_BYTES: usize = 16 * 1024;
pub const MAX_BUFFERED_BYTES: u64 = 256 * 1024;

#[derive(Debug)]
pub enum TextPayload {
    WebSocket(Vec<u8>),
    WebRtc(String),
}
impl TextPayload {
    pub fn len(&self) -> usize {
        match self {
            Self::WebSocket(bytes) => bytes.len(),
            Self::WebRtc(text) => text.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
#[derive(Debug)]
pub struct TextRequest {
    pub id: String,
    pub payload: TextPayload,
}

#[derive(Debug)]
pub struct TextEntry {
    session: u64,
    ready: bool,
    closed: bool,
    next: u64,
}
impl TextEntry {
    pub fn new(session: u64) -> Self {
        Self {
            session,
            ready: false,
            closed: false,
            next: 1,
        }
    }
    /// Call only from the current session's transport callback. The correlation
    /// check also rejects a stale ready response delivered on the new transport.
    pub fn accept_ready(&mut self, message: &Value) -> bool {
        if !self.closed
            && message["label"] == "rtvi-ai"
            && message["type"] == "bot-ready"
            && message["id"].as_str() == Some(self.session.to_string().as_str())
            && message["data"]["version"] == "2.2.0"
        {
            self.ready = true;
            true
        } else {
            false
        }
    }
    pub fn is_ready(&self) -> bool {
        self.ready && !self.closed
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.ready = false;
    }
    /// No pending queue is kept. An allocated ID is consumed even if the caller's
    /// synchronous transport send fails; retry must be a new explicit submission.
    pub fn prepare(
        &mut self,
        text: &str,
        transport: Transport,
        buffered_bytes: u64,
    ) -> Result<TextRequest, String> {
        if !self.is_ready() {
            return Err("Wait for this session's bot-ready before sending text".into());
        }
        if text.trim().is_empty() {
            return Err("Enter a nonblank message".into());
        }
        if text.len() > MAX_TEXT_BYTES {
            return Err("Message exceeds 16 KiB of UTF-8 text".into());
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or("Text request IDs exhausted")?;
        let id = format!("text-{}-{}", self.session, self.next);
        let value = json!({"id":id,"label":"rtvi-ai","type":"send-text","data":{
            "content":text,"options":{"run_immediately":true,"audio_response":true}
        }});
        let payload = match transport {
            Transport::WebSocket => TextPayload::WebSocket(protocol::encode_message(&value)),
            Transport::SmallWebRtc => TextPayload::WebRtc(value.to_string()),
            _ => return Err("Text transport is unsupported".into()),
        };
        let wire_limit = if transport == Transport::SmallWebRtc {
            MAX_WEBRTC_TEXT_WIRE_BYTES
        } else {
            MAX_TEXT_WIRE_BYTES
        };
        if payload.len() > wire_limit {
            return Err("Encoded text request exceeds this transport's message limit (WebRTC 16 KiB; WebSocket 128 KiB)".into());
        }
        if buffered_bytes
            .checked_add(payload.len() as u64)
            .is_none_or(|total| total > MAX_BUFFERED_BYTES)
        {
            return Err("Transport is busy; draft kept, send again when ready".into());
        }
        self.next = next;
        Ok(TextRequest { id, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ready(id: u64) -> Value {
        json!({"label":"rtvi-ai","id":id.to_string(),"type":"bot-ready","data":{"version":"2.2.0"}})
    }
    fn value(request: &TextRequest) -> Value {
        match &request.payload {
            TextPayload::WebRtc(text) => serde_json::from_str(text).unwrap(),
            TextPayload::WebSocket(bytes) => {
                let Some(protocol::frame::Body::Message(message)) =
                    protocol::decode(bytes).unwrap().body
                else {
                    panic!()
                };
                serde_json::from_str(&message.data).unwrap()
            }
        }
    }
    #[test]
    fn only_exact_ready_unlocks_current_session_and_close_is_permanent() {
        let mut entry = TextEntry::new(9);
        assert!(entry.prepare("hello", Transport::WebSocket, 0).is_err());
        assert!(!entry.accept_ready(&ready(8)));
        for (pointer, replacement) in [
            ("/label", json!("other")),
            ("/type", json!("client-ready")),
            ("/id", json!(9)),
            ("/data/version", json!("2.1.0")),
            ("/data/version", Value::Null),
        ] {
            let mut wrong = ready(9);
            *wrong.pointer_mut(pointer).unwrap() = replacement;
            assert!(!entry.accept_ready(&wrong));
            assert!(!entry.is_ready());
        }
        assert!(entry.accept_ready(&ready(9)));
        assert!(entry.is_ready());
        entry.close();
        entry.close();
        assert!(!entry.accept_ready(&ready(9)));
        assert!(entry.prepare("hello", Transport::WebSocket, 0).is_err());
        let mut fresh = TextEntry::new(11);
        assert!(!fresh.accept_ready(&ready(9)));
        assert!(fresh.accept_ready(&ready(11)));
        assert_eq!(
            fresh.prepare("hello", Transport::WebSocket, 0).unwrap().id,
            "text-11-1"
        );
    }
    #[test]
    fn both_wire_encodings_preserve_chinese_emoji_and_literal_text() {
        let text = "  你好，世界！🦀\n\"literal\" <script> & ";
        for transport in [Transport::WebSocket, Transport::SmallWebRtc] {
            let mut entry = TextEntry::new(3);
            entry.accept_ready(&ready(3));
            let request = entry.prepare(text, transport, 0).unwrap();
            let v = value(&request);
            assert_eq!(
                v,
                json!({"id":"text-3-1","label":"rtvi-ai","type":"send-text","data":{"content":text,"options":{"run_immediately":true,"audio_response":true}}})
            );
            assert!(request.payload.len() < MAX_TEXT_WIRE_BYTES);
        }
    }
    #[test]
    fn blank_and_utf8_limits_reject_without_truncation_or_id_consumption() {
        let mut entry = TextEntry::new(1);
        entry.accept_ready(&ready(1));
        for text in [
            String::new(),
            " \n\t\u{3000}".into(),
            "中".repeat(MAX_TEXT_BYTES / 3 + 1),
            "x".repeat(MAX_TEXT_BYTES + 1),
        ] {
            assert!(entry.prepare(&text, Transport::WebSocket, 0).is_err());
        }
        let text = "x".repeat(MAX_TEXT_BYTES);
        let request = entry.prepare(&text, Transport::WebSocket, 0).unwrap();
        assert_eq!(request.id, "text-1-1");
        assert_eq!(value(&request)["data"]["content"], text);
    }
    #[test]
    fn actual_encoded_bytes_and_checked_sum_bound_both_transports() {
        // Control bytes expand to six JSON bytes. Budget the wire representation,
        // not just the much smaller original UTF-8 input.
        for transport in [Transport::WebSocket, Transport::SmallWebRtc] {
            let raw_bytes = if transport == Transport::WebSocket {
                MAX_TEXT_BYTES
            } else {
                2000
            };
            let text = "\u{0001}".repeat(raw_bytes);
            let mut entry = TextEntry::new(1);
            entry.accept_ready(&ready(1));
            let n = entry.prepare(&text, transport, 0).unwrap().payload.len() as u64;
            assert!(n > 6 * raw_bytes as u64 && n < MAX_TEXT_WIRE_BYTES as u64);
            assert!(entry
                .prepare(&text, transport, MAX_BUFFERED_BYTES - n)
                .is_ok());
            assert!(entry
                .prepare(&text, transport, MAX_BUFFERED_BYTES - n + 1)
                .is_err());
            assert!(entry.prepare("x", transport, u64::MAX).is_err());
        }
    }
    #[test]
    fn webrtc_encoded_exact_limit_and_one_over_reject_before_allocating_id() {
        let mut measure = TextEntry::new(1);
        measure.accept_ready(&ready(1));
        let overhead = measure
            .prepare("x", Transport::SmallWebRtc, 0)
            .unwrap()
            .payload
            .len()
            - 1;
        let exact = "x".repeat(MAX_WEBRTC_TEXT_WIRE_BYTES - overhead);
        let over = format!("{exact}x");
        assert!(over.len() <= MAX_TEXT_BYTES);
        let mut entry = TextEntry::new(1);
        entry.accept_ready(&ready(1));
        assert!(entry.prepare(&over, Transport::SmallWebRtc, 0).is_err());
        assert!(entry
            .prepare(&"\u{0001}".repeat(3000), Transport::SmallWebRtc, 0)
            .is_err());
        assert_eq!(entry.next, 1);
        let request = entry.prepare(&exact, Transport::SmallWebRtc, 0).unwrap();
        assert_eq!(request.id, "text-1-1");
        assert_eq!(request.payload.len(), MAX_WEBRTC_TEXT_WIRE_BYTES);
        assert_eq!(value(&request)["data"]["content"], exact);
    }

    #[test]
    fn intentional_repeats_have_new_ids_and_no_pending_replay() {
        let mut entry = TextEntry::new(5);
        entry.accept_ready(&ready(5));
        let first = entry.prepare("repeat", Transport::WebSocket, 0).unwrap();
        // Dropping an unsent request performs no retry and retains no payload.
        assert_eq!(first.id, "text-5-1");
        drop(first);
        assert_eq!(
            entry.prepare("repeat", Transport::WebSocket, 0).unwrap().id,
            "text-5-2"
        );
        assert!(entry.prepare("", Transport::WebSocket, 0).is_err());
        assert_eq!(
            entry.prepare("repeat", Transport::WebSocket, 0).unwrap().id,
            "text-5-3"
        );
        entry.next = u64::MAX;
        assert!(entry.prepare("repeat", Transport::WebSocket, 0).is_err());
    }
}
