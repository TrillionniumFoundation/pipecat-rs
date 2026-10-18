//! Small local-file and source fixtures shared by enabled and disabled targets.
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub const WAIT: Duration = Duration::from_secs(8);
pub const SECRET: &str = "synthetic-local-cartesia-key-never-print-121";
pub const REFERENCE: &str = " Exact CaSe user reference never injected into RTVI ";
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub struct Bundle(pub PathBuf);
impl Bundle {
    pub fn new() -> Self {
        use std::os::unix::fs::DirBuilderExt;
        let p = std::env::temp_dir().join(format!(
            "pipecat-user-speech-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
        Self(p)
    }
    pub fn put(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let p = self.0.join(name);
        std::fs::write(&p, bytes).unwrap();
        p
    }
    pub fn json(&self, name: &str, value: &Value) -> PathBuf {
        self.put(name, serde_json::to_vec(value).unwrap())
    }
    pub fn cache(&self) -> PathBuf {
        use std::os::unix::fs::DirBuilderExt;
        let p = self.0.join("cache");
        std::fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
        p
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn expected(n: usize) -> Value {
    json!({"event":"llm_response","text_contains":format!("complete PCM {n}"),"within_ms":4000})
}
pub fn source() -> Value {
    json!({"name":"Cartesia complete PCM fixture", "user":{"modality":"audio", "speech":{"service":"cartesia","voice":"Fixture-VOICE"}}, "turns":[{"user":REFERENCE,"expect":[expected(1)]}]})
}
