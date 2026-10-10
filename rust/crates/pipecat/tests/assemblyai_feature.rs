//! Public facade availability without connecting to a provider.
#![cfg(feature = "assemblyai")]
use pipecat::assemblyai::{sync, AssemblyAiSttProcessor, StreamingSettings};

#[test]
fn facade_exposes_streaming_and_segmented_sync_native_apis() {
    let _ = StreamingSettings::default();
    let _ = sync::AssemblyAiSyncSettings::default();
    assert!(std::mem::size_of::<AssemblyAiSttProcessor>() > 0);
    assert!(std::mem::size_of::<sync::AssemblyAiSyncProcessor>() > 0);
}
