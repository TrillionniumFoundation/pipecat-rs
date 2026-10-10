use crate::{Result, VadError};
use std::{path::PathBuf, sync::Mutex};

/// ONNX Runtime is process-global. Initialize this crate before another `ort`
/// consumer and use the same library path for every model in the process.
#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub library_path: PathBuf,
}
static RUNTIME: Mutex<Option<PathBuf>> = Mutex::new(None);

pub(crate) fn initialize(options: &RuntimeOptions) -> Result<()> {
    let path = options.library_path.canonicalize().map_err(|error| {
        VadError::Runtime(format!(
            "ONNX Runtime {}: {error}",
            options.library_path.display()
        ))
    })?;
    if !path.is_file() {
        return Err(VadError::Runtime(
            "ONNX Runtime must be a library file".into(),
        ));
    }
    let mut initialized = RUNTIME
        .lock()
        .map_err(|_| VadError::Runtime("runtime lock poisoned".into()))?;
    if let Some(existing) = &*initialized {
        return if existing == &path {
            Ok(())
        } else {
            Err(VadError::Runtime(format!(
                "ONNX Runtime is already initialized from {}",
                existing.display()
            )))
        };
    }
    // ort's dynamic loader panics on an incompatible or corrupt library. Turn
    // that into a startup error; native model failures must not become silence.
    let outcome = std::panic::catch_unwind(|| {
        ort::init_from(path.to_string_lossy())
            .with_name("pipecat-vad")
            .with_telemetry(false)
            .commit()
    })
    .map_err(|_| {
        VadError::Runtime(
            "cannot load ONNX Runtime; require the 1.20 API or newer compatible runtime".into(),
        )
    })?;
    outcome.map_err(|error| VadError::Runtime(error.to_string()))?;
    *initialized = Some(path);
    Ok(())
}
