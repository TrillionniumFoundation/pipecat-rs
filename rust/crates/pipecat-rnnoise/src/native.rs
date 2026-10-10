//! The only unsafe boundary. Each native state belongs to one worker thread.
use crate::{Engine, Result, RnNoiseConfig, RnNoiseError, FRAME_SAMPLES};
use libloading::Library;
use sha2::{Digest, Sha256};
use std::{ffi::c_void, io::Read, ptr::NonNull, sync::Mutex};

type Create = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
type Destroy = unsafe extern "C" fn(*mut c_void);
type Process = unsafe extern "C" fn(*mut c_void, *mut f32, *const f32) -> f32;
type FrameSize = unsafe extern "C" fn() -> std::ffi::c_int;

// RNNoise 0.1.1 lazily initializes shared FFT/window tables without synchronization.
// Protect all calls, including construction/destruction, across all our instances.
// Other consumers of the same library must coordinate their own initialization.
static NATIVE_CALL: Mutex<()> = Mutex::new(());

pub(crate) struct NativeEngine {
    state: NonNull<c_void>,
    destroy: Destroy,
    process: Process,
    _library: Library,
}
impl NativeEngine {
    pub(crate) fn load(config: &RnNoiseConfig) -> Result<Self> {
        let path = config.library_path.canonicalize()?;
        let file = std::fs::File::open(&path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > 64 * 1024 * 1024 {
            return Err(RnNoiseError::Invalid(
                "native library must be a regular file at most 64 MiB",
            ));
        }
        let mut bytes = Vec::new();
        file.take(64 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(RnNoiseError::TooLarge);
        }
        let actual = format!("{:x}", Sha256::digest(&bytes));
        if actual != config.library_sha256.to_ascii_lowercase() {
            return Err(RnNoiseError::HashMismatch);
        }
        let _guard = NATIVE_CALL.lock().map_err(|_| RnNoiseError::Worker)?;
        // SAFETY: config explicitly identifies caller-trusted, immutable native code.
        // Its transitive dependencies are also the caller's trust responsibility.
        let library = unsafe { Library::new(&path) }.map_err(native_error)?;
        // SAFETY: these signatures are the public Xiph RNNoise C ABI. A hash alone
        // cannot prove them; the trusted artifact must implement the documented ABI.
        let (create, destroy, process, frame_size) = unsafe {
            (
                *library
                    .get::<Create>(b"rnnoise_create\0")
                    .map_err(native_error)?,
                *library
                    .get::<Destroy>(b"rnnoise_destroy\0")
                    .map_err(native_error)?,
                *library
                    .get::<Process>(b"rnnoise_process_frame\0")
                    .map_err(native_error)?,
                *library
                    .get::<FrameSize>(b"rnnoise_get_frame_size\0")
                    .map_err(native_error)?,
            )
        };
        // SAFETY: library is alive and the resolved function takes no parameters.
        if unsafe { frame_size() } != FRAME_SAMPLES as i32 {
            return Err(RnNoiseError::Native(
                "expected 480-sample RNNoise ABI".into(),
            ));
        }
        // SAFETY: NULL selects the built-in model; ownership transfers to this value.
        let state = NonNull::new(unsafe { create(std::ptr::null_mut()) })
            .ok_or_else(|| RnNoiseError::Native("rnnoise_create returned NULL".into()))?;
        Ok(Self {
            state,
            destroy,
            process,
            _library: library,
        })
    }
}
impl Engine for NativeEngine {
    fn process(&mut self, samples: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]> {
        let input = samples.map(f32::from);
        let mut output = [0.0_f32; FRAME_SAMPLES];
        let _guard = NATIVE_CALL.lock().map_err(|_| RnNoiseError::Worker)?;
        // SAFETY: state is exclusively owned, buffers are 480 valid floats, they
        // do not alias, and the containing Library outlives every call and Drop.
        let probability =
            unsafe { (self.process)(self.state.as_ptr(), output.as_mut_ptr(), input.as_ptr()) };
        if !probability.is_finite()
            || !(0.0..=1.0).contains(&probability)
            || output.iter().any(|value| !value.is_finite())
        {
            return Err(RnNoiseError::Native(
                "invalid non-finite output or speech probability".into(),
            ));
        }
        // RNNoise floats have PCM16 amplitude, not normalized amplitude. Truncate
        // toward zero like pyrnnoise, and saturate overflow instead of wrapping.
        Ok(output.map(|value| value.clamp(-32768.0, 32767.0) as i16))
    }
}
impl Drop for NativeEngine {
    fn drop(&mut self) {
        let _guard = NATIVE_CALL
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // SAFETY: this is the unique live state, created by this loaded library.
        unsafe { (self.destroy)(self.state.as_ptr()) };
    }
}
fn native_error(error: libloading::Error) -> RnNoiseError {
    RnNoiseError::Native(error.to_string())
}
