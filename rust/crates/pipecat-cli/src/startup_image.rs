//! One trusted, already decoded RGB8 startup asset. No codec or URL loader.
use crate::{receipt_output::ImageChannelConfig, AgentError};
use pipecat_core::ImageRawFrame;
use std::{fmt, fs::File, io::Read, path::Path, sync::Arc};

/// Immutable bounded pixels, shared by Options clones and copied once per session.
#[derive(Clone, PartialEq, Eq)]
pub struct StartupImage(Arc<ImageRawFrame>);
impl fmt::Debug for StartupImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartupImage")
            .field("size", &self.0.size)
            .field("pixel_bytes", &self.0.image.len())
            .finish_non_exhaustive()
    }
}
impl StartupImage {
    /// Tightly packed row-major RGB8, with no header or padding.
    pub fn from_rgb(pixels: Vec<u8>, size: (u32, u32)) -> Result<Self, AgentError> {
        let image = Self(Arc::new(ImageRawFrame {
            image: pixels,
            size,
            format: Some("RGB".into()),
        }));
        image.validate(&ImageChannelConfig::default())?;
        Ok(image)
    }
    /// Load a stable trusted regular local file before starting session owners.
    /// Reads are byte-bounded, not time-bounded. Concurrent path/file mutation is
    /// unsupported; this is not a sandbox or an atomic filesystem snapshot.
    pub fn load_rgb(path: impl AsRef<Path>, size: (u32, u32)) -> Result<Self, AgentError> {
        let expected = size_bytes(size, &ImageChannelConfig::default())?;
        let path = path.as_ref();
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| AgentError::Config("cannot inspect initial RGB image file"))?;
        if !metadata.file_type().is_file() || metadata.len() != expected as u64 {
            return Err(AgentError::Config(
                "initial RGB image needs a regular file with exact pixel length",
            ));
        }
        let mut file = File::open(path)
            .map_err(|_| AgentError::Config("cannot open initial RGB image file"))?;
        let metadata = file
            .metadata()
            .map_err(|_| AgentError::Config("cannot inspect opened initial RGB image file"))?;
        if !metadata.is_file() || metadata.len() != expected as u64 {
            return Err(AgentError::Config(
                "initial RGB image needs a regular file with exact pixel length",
            ));
        }
        let mut pixels = vec![0; expected];
        file.read_exact(&mut pixels)
            .map_err(|_| AgentError::Config("initial RGB image is truncated or unreadable"))?;
        let mut trailing = [0];
        if file
            .read(&mut trailing)
            .map_err(|_| AgentError::Config("cannot finish initial RGB image read"))?
            != 0
        {
            return Err(AgentError::Config(
                "initial RGB image contains trailing bytes",
            ));
        }
        Self::from_rgb(pixels, size)
    }
    pub(crate) fn validate(&self, config: &ImageChannelConfig) -> Result<(), AgentError> {
        let expected = size_bytes(self.0.size, config)?;
        if self.0.image.len() != expected || self.0.image.capacity() > config.max_pixel_bytes {
            return Err(AgentError::Config(
                "initial RGB image length or retained allocation exceeds its budget",
            ));
        }
        Ok(())
    }
    pub(crate) fn frame(&self) -> ImageRawFrame {
        (*self.0).clone()
    }
}
fn size_bytes(
    (width, height): (u32, u32),
    config: &ImageChannelConfig,
) -> Result<usize, AgentError> {
    let bytes = (width as usize)
        .checked_mul(height as usize)
        .and_then(|n| n.checked_mul(3));
    if width == 0 || height == 0 || width > config.max_dimension || height > config.max_dimension {
        return Err(AgentError::Config(
            "initial RGB image dimensions exceed its budget",
        ));
    }
    bytes
        .filter(|n| *n <= config.max_pixel_bytes)
        .ok_or(AgentError::Config(
            "initial RGB image pixel size exceeds its budget",
        ))
}
