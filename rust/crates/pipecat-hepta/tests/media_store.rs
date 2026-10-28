#![cfg(unix)]
//! Actual private-file tests with synthetic PCM; no microphone/model acceptance.
use pipecat_hepta::{MediaStore, MediaStoreConfig, MediaStoreError, StreamIdentity};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hepta-media-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
    fn config(&self, generation: u64) -> MediaStoreConfig {
        MediaStoreConfig {
            directory: self.0.clone(),
            capture_generation: generation,
            max_assets: 2,
            max_bytes: 8,
            max_segment_bytes: 8,
        }
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn stream() -> StreamIdentity {
    StreamIdentity {
        principal_id: "user:test".into(),
        device_id: "device:test".into(),
        device_generation: 2,
        organ_id: "hmi:test".into(),
        organ_generation: 7,
        logical_session_id: "logical:test".into(),
    }
}

#[test]
fn exact_bytes_span_digest_and_scope_are_verified_before_release() {
    let root = Root::new();
    let store = MediaStore::open(root.config(1), stream()).unwrap();
    let source = store.retain(b"abcd", 1).unwrap();
    assert_eq!(
        source.sha256,
        "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
    );
    assert_eq!(
        (source.byte_length, source.start_sample, source.end_sample),
        (4, 0, 2)
    );
    store.verify(&source).unwrap();
    let mut foreign = source.clone();
    foreign.stream.device_generation += 1;
    assert!(matches!(
        store.verify(&foreign),
        Err(MediaStoreError::Identity)
    ));
    let mut wrong_span = source.clone();
    wrong_span.end_sample += 1;
    assert!(matches!(
        store.verify(&wrong_span),
        Err(MediaStoreError::Identity)
    ));
    store.release(&source).unwrap();
    assert!(!root.0.join(&source.asset_id).exists());
    assert!(matches!(
        store.retain(b"abcd", 1),
        Err(MediaStoreError::Identity)
    ));
}

#[test]
fn quota_survives_reopen_and_only_an_acknowledged_asset_frees_capacity() {
    let root = Root::new();
    let store = MediaStore::open(root.config(1), stream()).unwrap();
    let first = store.retain(b"abcd", 1).unwrap();
    store.retain(b"efgh", 2).unwrap();
    assert!(matches!(
        store.retain(b"ij", 3),
        Err(MediaStoreError::Backpressure)
    ));
    drop(store);
    assert!(matches!(
        MediaStore::open(root.config(1), stream()),
        Err(MediaStoreError::Identity)
    ));
    let recovered = MediaStore::open(root.config(2), stream()).unwrap();
    assert!(matches!(
        recovered.retain(b"ij", 1),
        Err(MediaStoreError::Backpressure)
    ));
    recovered.verify(&first).unwrap();
    recovered.release(&first).unwrap();
    let next = recovered.retain(b"ij", 1).unwrap();
    assert_ne!(next.asset_id, first.asset_id);
}

#[test]
fn corrupt_bytes_and_foreign_root_binding_never_become_valid_observations() {
    let root = Root::new();
    let store = MediaStore::open(root.config(1), stream()).unwrap();
    let source = store.retain(b"abcd", 1).unwrap();
    fs::write(root.0.join(&source.asset_id), b"bad!").unwrap();
    assert!(matches!(
        store.verify(&source),
        Err(MediaStoreError::Identity)
    ));
    assert!(store.release(&source).is_err());
    let mut foreign = stream();
    foreign.principal_id = "other".into();
    assert!(matches!(
        MediaStore::open(root.config(2), foreign),
        Err(MediaStoreError::Identity)
    ));
}

#[test]
fn symlinks_and_path_escape_are_rejected_without_touching_the_target() {
    use std::os::unix::fs::symlink;
    let root = Root::new();
    let outside = Root::new();
    let target = outside.0.join("protected");
    fs::write(&target, b"abcd").unwrap();
    let store = MediaStore::open(root.config(1), stream()).unwrap();
    symlink(&target, root.0.join("capture-1-1.pcm16")).unwrap();
    assert!(store.retain(b"zz", 1).is_err());
    assert_eq!(fs::read(&target).unwrap(), b"abcd");
    fs::remove_file(root.0.join("capture-1-1.pcm16")).unwrap();
    let mut source = store.retain(b"abcd", 1).unwrap();
    source.asset_id = "../protected".into();
    assert!(store.verify(&source).is_err());
}

#[test]
fn duplicate_old_generation_release_preserves_new_assets_and_quota() {
    let root = Root::new();
    let old = MediaStore::open(root.config(1), stream()).unwrap();
    let source = old.retain(b"abcd", 1).unwrap();
    old.release(&source).unwrap();
    drop(old);
    let current = MediaStore::open(root.config(2), stream()).unwrap();
    let first = current.retain(b"abcd", 1).unwrap();
    let second = current.retain(b"efgh", 2).unwrap();
    assert!(current.release(&source).is_err());
    current.verify(&first).unwrap();
    current.verify(&second).unwrap();
    assert!(matches!(
        current.retain(b"ij", 3),
        Err(MediaStoreError::Backpressure)
    ));
    assert_eq!(fs::read(root.0.join(&first.asset_id)).unwrap(), b"abcd");
    assert_eq!(fs::read(root.0.join(&second.asset_id)).unwrap(), b"efgh");
    current.release(&first).unwrap();
    assert!(current.release(&first).is_err());
    current.retain(b"ij", 3).unwrap();
    current.verify(&second).unwrap();
}

#[test]
fn old_organ_reference_cannot_release_identically_named_new_asset() {
    let old_root = Root::new();
    let old = MediaStore::open(old_root.config(1), stream()).unwrap();
    let old_source = old.retain(b"abcd", 1).unwrap();
    let root = Root::new();
    let mut new_stream = stream();
    new_stream.organ_generation += 1;
    let current = MediaStore::open(root.config(1), new_stream).unwrap();
    let new_source = current.retain(b"abcd", 1).unwrap();
    assert_eq!(old_source.asset_id, new_source.asset_id);
    assert_eq!(old_source.sha256, new_source.sha256);
    assert!(matches!(
        current.release(&old_source),
        Err(MediaStoreError::Identity)
    ));
    current.verify(&new_source).unwrap();
    assert_eq!(
        fs::read(root.0.join(&new_source.asset_id)).unwrap(),
        b"abcd"
    );
}

#[cfg(feature = "local-speech")]
#[test]
fn unpublished_cleanup_validates_full_identity_and_releases_once() {
    use pipecat_whisper::AudioSourceRetainer;
    let root = Root::new();
    let store = MediaStore::open(root.config(1), stream()).unwrap();
    let metadata = store.retain_source(b"abcd", 1).unwrap();
    let source: pipecat_hepta::CapturedAudioRef = serde_json::from_value(metadata.clone()).unwrap();
    let mut foreign = source.clone();
    foreign.stream.organ_generation += 1;
    assert!(store
        .release_unpublished_source(&serde_json::to_value(foreign).unwrap())
        .is_err());
    store.verify(&source).unwrap();
    store.release_unpublished_source(&metadata).unwrap();
    assert!(store.release_unpublished_source(&metadata).is_err());
    let next = store.retain(b"efgh", 2).unwrap();
    store.verify(&next).unwrap();
    assert!(store.release_unpublished_source(&metadata).is_err());
    store.verify(&next).unwrap();
}
