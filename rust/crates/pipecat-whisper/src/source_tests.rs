//! Source-callback fixtures, not native-model or microphone qualification.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct Retainer {
    calls: AtomicUsize,
    bytes: Mutex<Vec<u8>>,
    cancel: Option<Cancellation>,
    fail: bool,
}
impl AudioSourceRetainer for Retainer {
    fn retain_source(&self, pcm16: &[u8], sequence: u64) -> Result<serde_json::Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.bytes.lock().unwrap() = pcm16.to_vec();
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        if self.fail {
            return Err(WhisperError::Worker("source quota exhausted".into()));
        }
        Ok(serde_json::json!({"sequence":sequence,"bytes":pcm16.len()}))
    }
}
fn transcript(text: &str) -> Transcript {
    Transcript {
        text: text.into(),
        language: None,
        segments: vec![],
        audio_duration_seconds: 0.001,
        audio_source: None,
    }
}
fn retainer(cancel: Option<Cancellation>, fail: bool) -> Arc<Retainer> {
    Arc::new(Retainer {
        calls: AtomicUsize::new(0),
        bytes: Mutex::new(vec![]),
        cancel,
        fail,
    })
}
fn capture(retainer: Arc<Retainer>) -> SourceCapture {
    SourceCapture {
        retainer,
        pcm16: b"abcd".to_vec(),
        sequence: 7,
    }
}

#[test]
fn exact_captured_bytes_and_sequence_are_attached_without_decoder_padding() {
    let store = retainer(None, false);
    let result = attach_source(
        transcript("fixture decode"),
        Some(capture(store.clone())),
        &Cancellation::new(),
        &Cancellation::new(),
    )
    .unwrap();
    assert_eq!(
        result.audio_source,
        Some(serde_json::json!({"sequence":7,"bytes":4}))
    );
    assert_eq!(*store.bytes.lock().unwrap(), b"abcd");
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelled_work_never_calls_retention_and_empty_text_has_no_asset() {
    let store = retainer(None, false);
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        attach_source(
            transcript("fixture decode"),
            Some(capture(store.clone())),
            &cancelled,
            &Cancellation::new()
        ),
        Err(WhisperError::Cancelled)
    ));
    let empty = attach_source(
        transcript(""),
        Some(capture(store.clone())),
        &Cancellation::new(),
        &Cancellation::new(),
    )
    .unwrap();
    assert_eq!(empty.audio_source, None);
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn retention_error_and_cancel_during_storage_do_not_emit_a_final_observation() {
    let failed = retainer(None, true);
    assert!(
        matches!(attach_source(transcript("fixture decode"), Some(capture(failed)),
        &Cancellation::new(), &Cancellation::new()), Err(WhisperError::Worker(message)) if message == "source quota exhausted")
    );
    let cancel = Cancellation::new();
    let store = retainer(Some(cancel.clone()), false);
    assert!(matches!(
        attach_source(
            transcript("fixture decode"),
            Some(capture(store)),
            &cancel,
            &Cancellation::new()
        ),
        Err(WhisperError::Cancelled)
    ));
}
