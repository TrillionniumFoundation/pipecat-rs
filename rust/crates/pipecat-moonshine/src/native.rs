//! Audited Moonshine 0.1.5 C ABI. Struct layout is pinned by native binary hashes.
use crate::{config::NativeLibraries, MoonshineError, Result};
use libloading::Library;
use std::{
    ffi::{c_char, c_void, CString},
    ptr,
};

#[repr(C)]
struct OptionC {
    name: *const c_char,
    value: *const c_char,
}
#[repr(C)]
struct TranscriptLineC {
    text: *const c_char,
    audio_data: *const f32,
    audio_data_count: usize,
    start: f32,
    duration: f32,
    id: u64,
    is_complete: i8,
    is_updated: i8,
    is_new: i8,
    has_text_changed: i8,
    have_speakers_changed: i8,
    speaker_spans: *const c_void,
    speaker_span_count: u64,
    last_transcription_latency_ms: u32,
    words: *const c_void,
    word_count: u64,
}
#[repr(C)]
struct TranscriptC {
    lines: *const TranscriptLineC,
    line_count: u64,
}
type Load = unsafe extern "C" fn(
    *const *const c_char,
    *const *const u8,
    *const u64,
    u64,
    u32,
    *const OptionC,
    u64,
    i32,
) -> i32;
type Free = unsafe extern "C" fn(i32);
type Transcribe =
    unsafe extern "C" fn(i32, *const f32, u64, i32, u32, *mut *const TranscriptC) -> i32;
type ErrorText = unsafe extern "C" fn(i32) -> *const c_char;

pub(crate) struct Engine {
    load: Load,
    free: Free,
    transcribe: Transcribe,
    error: ErrorText,
    // Drop libraries last, after all function pointers/native handles cease use.
    _library: Library,
    _runtime: Library,
    _snapshots: [std::fs::File; 2],
}
impl Engine {
    pub(crate) fn load(libraries: &NativeLibraries) -> Result<Self> {
        libraries.validate()?;
        let runtime_bytes = libraries.onnx_runtime.read(64 * 1024 * 1024)?;
        let library_bytes = libraries.moonshine.read(64 * 1024 * 1024)?;
        let runtime_file = sealed_snapshot(&runtime_bytes)?;
        let library_file = sealed_snapshot(&library_bytes)?;
        use std::os::fd::AsRawFd;
        let runtime_path = format!("/proc/self/fd/{}", runtime_file.as_raw_fd());
        let path = format!("/proc/self/fd/{}", library_file.as_raw_fd());
        // SAFETY: immutable sealed snapshots contain exactly the audited binary
        // bytes. RTLD_LOCAL and the wheel's unique ORT SONAME/symbol version keep
        // its runtime separate from ort consumers. No loader environment changes.
        let runtime = unsafe { Library::new(&runtime_path) }.map_err(native_error)?;
        let library = unsafe { Library::new(&path) }.map_err(native_error)?;
        // SAFETY: verified native ABI; every symbol is resolved before use and
        // the library remains alive until the engine and its handles are gone.
        let (load, free, transcribe, error, version) = unsafe {
            (
                *library
                    .get::<Load>(b"moonshine_load_transcriber_from_memory_files\0")
                    .map_err(native_error)?,
                *library
                    .get::<Free>(b"moonshine_free_transcriber\0")
                    .map_err(native_error)?,
                *library
                    .get::<Transcribe>(b"moonshine_transcribe_without_streaming\0")
                    .map_err(native_error)?,
                *library
                    .get::<ErrorText>(b"moonshine_error_to_string\0")
                    .map_err(native_error)?,
                *library
                    .get::<unsafe extern "C" fn() -> i32>(b"moonshine_get_version\0")
                    .map_err(native_error)?,
            )
        };
        // SAFETY: no arguments, validated symbol.
        if unsafe { version() } != 30000 {
            return Err(MoonshineError::Invalid(
                "Moonshine C ABI version 30000 is required",
            ));
        }
        Ok(Self {
            load,
            free,
            transcribe,
            error,
            _library: library,
            _runtime: runtime,
            _snapshots: [runtime_file, library_file],
        })
    }
    pub(crate) fn load_model(&self, files: &[(String, Vec<u8>)], arch: u32) -> Result<i32> {
        let names: Vec<CString> = files
            .iter()
            .map(|(n, _)| CString::new(n.as_str()).expect("validated filenames"))
            .collect();
        let names: Vec<_> = names.iter().map(|n| n.as_ptr()).collect();
        let buffers: Vec<_> = files.iter().map(|(_, b)| b.as_ptr()).collect();
        let sizes: Vec<u64> = files.iter().map(|(_, b)| b.len() as u64).collect();
        // CPU-only is explicit; no microphone, diarization or timestamp models.
        let option = OptionC {
            name: c"ort_providers".as_ptr(),
            value: c"CPU".as_ptr(),
        };
        // SAFETY: names, pointers and sizes have identical lengths. Owned verified
        // buffers remain at stable addresses through free_model; no empty buffers
        // are passed, preventing the ABI's fallback to filesystem reads.
        let handle = unsafe {
            (self.load)(
                names.as_ptr(),
                buffers.as_ptr(),
                sizes.as_ptr(),
                files.len() as u64,
                arch,
                &option,
                1,
                30000,
            )
        };
        if handle < 0 {
            Err(self.error(handle))
        } else {
            Ok(handle)
        }
    }
    pub(crate) fn free_model(&self, handle: i32) {
        // SAFETY: handle came from this engine and is freed exactly once.
        unsafe { (self.free)(handle) };
    }
    pub(crate) fn transcribe(
        &self,
        handle: i32,
        samples: &[f32],
        max: usize,
    ) -> Result<Vec<crate::TranscriptLine>> {
        let mut transcript = ptr::null();
        // SAFETY: valid live handle and samples; call is synchronous. Native owns
        // transcript pointers until next call or handle free. Copy before either.
        let status = unsafe {
            (self.transcribe)(
                handle,
                samples.as_ptr(),
                samples.len() as u64,
                16000,
                0,
                &mut transcript,
            )
        };
        if status != 0 {
            return Err(self.error(status));
        }
        // SAFETY: success returns a library-owned transcript or a rejected null.
        let transcript = unsafe { transcript.as_ref() }
            .ok_or_else(|| MoonshineError::Native("null transcript".into()))?;
        if transcript.line_count > 100_000 {
            return Err(MoonshineError::TooLarge);
        }
        if transcript.line_count == 0 {
            return Ok(vec![]);
        }
        if transcript.lines.is_null() {
            return Err(MoonshineError::Native("null transcript lines".into()));
        }
        // SAFETY: ABI guarantees line_count contiguous entries with audited stride.
        let lines =
            unsafe { std::slice::from_raw_parts(transcript.lines, transcript.line_count as usize) };
        let mut result = Vec::with_capacity(lines.len());
        let mut bytes = 0usize;
        for line in lines {
            if line.text.is_null()
                || !line.start.is_finite()
                || !line.duration.is_finite()
                || line.start < 0.0
                || line.duration < 0.0
            {
                return Err(MoonshineError::Native("invalid transcript line".into()));
            }
            // SAFETY: vendor owns the NUL-terminated string; scan no more than
            // the configured result budget before creating an owned string.
            let text = unsafe { bounded_text(line.text, max.saturating_sub(bytes)) }?;
            bytes = bytes.saturating_add(text.len()).saturating_add(1);
            if bytes > max {
                return Err(MoonshineError::TooLarge);
            }
            result.push(crate::TranscriptLine {
                text,
                start_seconds: line.start,
                duration_seconds: line.duration,
                id: line.id,
                is_complete: line.is_complete != 0,
            });
        }
        Ok(result)
    }
    fn error(&self, code: i32) -> MoonshineError {
        // SAFETY: error string has library lifetime and is guaranteed terminated.
        let ptr = unsafe { (self.error)(code) };
        let detail = if ptr.is_null() {
            "unknown native error".into()
        } else {
            unsafe { bounded_text(ptr, 4096) }
                .unwrap_or_else(|_| "invalid native error text".into())
        };
        MoonshineError::Native(format!("{code}: {detail}"))
    }
}
fn native_error(error: libloading::Error) -> MoonshineError {
    MoonshineError::Native(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audited_64_bit_abi_layout_matches_vendor_binding() {
        assert_eq!(std::mem::size_of::<TranscriptLineC>(), 88);
        assert_eq!(std::mem::align_of::<TranscriptLineC>(), 8);
        assert_eq!(std::mem::offset_of!(TranscriptLineC, speaker_spans), 48);
        assert_eq!(std::mem::offset_of!(TranscriptLineC, words), 72);
        assert_eq!(std::mem::size_of::<TranscriptC>(), 16);
    }
}

// SAFETY: caller guarantees ptr points to a vendor-owned NUL-terminated string.
unsafe fn bounded_text(ptr: *const c_char, max: usize) -> Result<String> {
    let mut len = 0;
    while len <= max {
        // SAFETY: valid native C string; stop at NUL before crossing its allocation.
        if unsafe { *ptr.add(len) } == 0 {
            // SAFETY: the preceding scan established this many accessible bytes.
            let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
            return std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| MoonshineError::Native("non-UTF-8 transcript".into()));
        }
        len += 1;
    }
    Err(MoonshineError::TooLarge)
}

#[cfg(target_os = "linux")]
fn sealed_snapshot(bytes: &[u8]) -> Result<std::fs::File> {
    use std::{
        io::Write,
        os::fd::{AsRawFd, FromRawFd},
    };
    // SAFETY: static NUL-terminated name and documented Linux flags.
    let fd = unsafe {
        libc::memfd_create(
            c"pipecat-moonshine".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: memfd_create returned a fresh owned descriptor.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.write_all(bytes)?;
    // SAFETY: valid descriptor; seal mutation, resizing and removal of seals.
    if unsafe {
        libc::fcntl(
            file.as_raw_fd(),
            libc::F_ADD_SEALS,
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(file)
}
#[cfg(not(target_os = "linux"))]
fn sealed_snapshot(_: &[u8]) -> Result<std::fs::File> {
    Err(MoonshineError::Invalid(
        "Linux sealed library snapshots required",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod snapshot_tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};
    #[test]
    fn executable_snapshot_is_immutable() {
        let mut file = sealed_snapshot(b"verified bytes").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(
            file.write(b"replacement").unwrap_err().raw_os_error(),
            Some(libc::EPERM)
        );
        assert_eq!(
            file.set_len(1).unwrap_err().raw_os_error(),
            Some(libc::EPERM)
        );
        let mut text = String::new();
        file.read_to_string(&mut text).unwrap();
        assert_eq!(text, "verified bytes");
    }
    #[test]
    fn native_text_is_bounded_before_copy_and_rejects_invalid_utf8() {
        let text = c"abcd";
        assert_eq!(unsafe { bounded_text(text.as_ptr(), 4) }.unwrap(), "abcd");
        assert!(matches!(
            unsafe { bounded_text(text.as_ptr(), 3) },
            Err(MoonshineError::TooLarge)
        ));
        let invalid = [255_u8, 0];
        assert!(unsafe { bounded_text(invalid.as_ptr().cast(), 1) }.is_err());
    }
}
