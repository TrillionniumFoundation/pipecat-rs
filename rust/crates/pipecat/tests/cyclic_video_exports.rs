use pipecat::{
    audio::PcmFormat,
    core::{async_trait, Frame, ProcessorError},
    media_transport::{
        CyclicVideoOutputConfig, OutputBackend, OutputTransport, OutputTransportConfig,
        PlaybackReceipt,
    },
};
use std::sync::Arc;
struct ExplicitBackend;
#[async_trait]
impl OutputBackend for ExplicitBackend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
    fn supports_independent_video(&self) -> bool {
        true
    }
}
#[test]
fn facade_exports_explicit_cyclic_video_without_performing_io() {
    let output = OutputTransport::new(
        "facade-cyclic",
        OutputTransportConfig::default(),
        Arc::new(ExplicitBackend),
        None,
        Default::default(),
    )
    .unwrap()
    .with_cyclic_video(CyclicVideoOutputConfig {
        queue_capacity: 4,
        queue_byte_capacity: 65536,
        retained_image_capacity: 8,
        retained_byte_capacity: 262144,
        framerate: 30,
    })
    .unwrap();
    assert!(!output.control().is_ready());
}
