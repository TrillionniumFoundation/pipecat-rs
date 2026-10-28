//! Source-callback fixtures, not native-model or microphone qualification.
use super::*;
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct Retainer {
    calls: AtomicUsize,
    bytes: Mutex<Vec<u8>>,
    cancel: Option<Cancellation>,
    fail: bool,
    live: Mutex<BTreeSet<u64>>,
    releases: AtomicUsize,
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
        self.live.lock().unwrap().insert(sequence);
        Ok(serde_json::json!({"sequence":sequence,"bytes":pcm16.len()}))
    }
    fn release_unpublished_source(&self, source: &serde_json::Value) -> Result<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        let sequence = source["sequence"].as_u64().unwrap();
        assert!(self.live.lock().unwrap().remove(&sequence));
        Ok(())
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
        live: Mutex::new(BTreeSet::new()),
        releases: AtomicUsize::new(0),
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

#[test]
fn cancel_during_retention_reclaims_the_unpublished_asset() {
    let cancel = Cancellation::new();
    let store = retainer(Some(cancel.clone()), false);
    assert!(matches!(
        attach_source(
            transcript("fixture decode"),
            Some(capture(store.clone())),
            &cancel,
            &Cancellation::new()
        ),
        Err(WhisperError::Cancelled)
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert!(
        store.live.lock().unwrap().is_empty(),
        "cancelled source lost its only release owner"
    );
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_an_unpublished_source_result_reclaims_its_asset() {
    let store = retainer(None, false);
    let result = attach_source(
        transcript("fixture decode"),
        Some(capture(store.clone())),
        &Cancellation::new(),
        &Cancellation::new(),
    )
    .unwrap();
    assert_eq!(store.live.lock().unwrap().len(), 1);
    drop(result);
    assert!(
        store.live.lock().unwrap().is_empty(),
        "discarded source result retained an unowned asset"
    );
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
}

#[test]
fn uncertain_handoff_preserves_source_after_consumer_cancellation() {
    let cancel = Cancellation::new();
    let store = retainer(None, false);
    let result = attach_source(
        transcript("fixture decode"),
        Some(capture(store.clone())),
        &cancel,
        &Cancellation::new(),
    )
    .unwrap();
    let transcript = result.handoff();
    cancel.cancel();
    drop(transcript);
    assert_eq!(*store.live.lock().unwrap(), BTreeSet::from([7]));
    assert_eq!(store.releases.load(Ordering::SeqCst), 0);
}

#[test]
fn oversized_source_metadata_reclaims_the_unpublished_asset() {
    struct Oversized(Arc<Retainer>);
    impl AudioSourceRetainer for Oversized {
        fn retain_source(&self, pcm16: &[u8], sequence: u64) -> Result<serde_json::Value> {
            self.0.retain_source(pcm16, sequence)?;
            Ok(serde_json::json!({"sequence":sequence,"padding":"x".repeat(4096)}))
        }
        fn release_unpublished_source(&self, source: &serde_json::Value) -> Result<()> {
            self.0.release_unpublished_source(source)
        }
    }
    let store = retainer(None, false);
    let source = SourceCapture {
        retainer: Arc::new(Oversized(store.clone())),
        pcm16: b"abcd".to_vec(),
        sequence: 7,
    };
    assert!(matches!(
        attach_source(
            transcript("fixture decode"),
            Some(source),
            &Cancellation::new(),
            &Cancellation::new()
        ),
        Err(WhisperError::Invalid(_))
    ));
    assert!(store.live.lock().unwrap().is_empty());
    assert_eq!(store.releases.load(Ordering::SeqCst), 1);
}
