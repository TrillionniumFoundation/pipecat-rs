//! Piper phonemization conventions adapted from rhasspy/piper-phonemize (MIT);
//! see licenses/piper-phonemize-MIT.txt and PROVENANCE.md.
//! Narrow native ABI boundary. Libraries must be trusted, caller supplied, and hashed.
use crate::{Artifact, PiperError, Result};
use libloading::Library;
use std::sync::Mutex;

static RUNTIME: Mutex<Option<(std::path::PathBuf, String, Library)>> = Mutex::new(None);
pub(crate) fn runtime(artifact: &Artifact) -> Result<()> {
    // Verification happens before dlopen; callers must not mutate artifacts during use.
    artifact.read(512 * 1024 * 1024)?;
    let path = artifact.path.canonicalize()?;
    let mut state = RUNTIME.lock().map_err(|_| PiperError::Worker)?;
    if let Some((loaded, hash, _)) = &*state {
        return if *loaded == path && *hash == artifact.sha256.to_ascii_lowercase() {
            Ok(())
        } else {
            Err(PiperError::Invalid(
                "ONNX runtime identity is already fixed for this process",
            ))
        };
    }
    // SAFETY: user explicitly supplies a trusted library; loaded ABI symbols are checked.
    let library = unsafe { Library::new(&path) }.map_err(|e| PiperError::Native(e.to_string()))?;
    // SAFETY: official ONNX Runtime C ABI. Keep library alive while using its pointers.
    let expected = unsafe {
        let get: libloading::Symbol<unsafe extern "system" fn() -> *const ort_sys::OrtApiBase> =
            library
                .get(b"OrtGetApiBase\0")
                .map_err(|e| PiperError::Native(e.to_string()))?;
        let base = get().as_ref().ok_or(PiperError::Invalid("null ONNX ABI"))?;
        base.GetApi
            .ok_or(PiperError::Invalid("missing ONNX GetApi"))?(ort_sys::ORT_API_VERSION)
    };
    if expected.is_null() {
        return Err(PiperError::Unsupported("ONNX Runtime API 20 is required"));
    }
    std::panic::catch_unwind(|| {
        ort::init_from(path.to_string_lossy())
            .with_name("pipecat-piper")
            .with_telemetry(false)
            .commit()
    })
    .map_err(|_| PiperError::Native("ONNX runtime loader panicked".into()))?
    .map_err(|e| PiperError::Native(e.to_string()))?;
    if !std::ptr::eq(expected, ort::api()) {
        return Err(PiperError::Invalid(
            "another ort consumer already selected a different native runtime",
        ));
    }
    *state = Some((path, artifact.sha256.to_ascii_lowercase(), library));
    Ok(())
}

#[cfg(feature = "espeak-gpl")]
mod espeak {
    use super::*;
    use crate::{config::normalized, sha256_directory, Cancellation, EspeakConfig};
    use std::{
        ffi::{c_char, c_int, c_void, CStr, CString},
        sync::TryLockError,
        time::Duration,
    };
    type Initialize = unsafe extern "C" fn(c_int, c_int, *const c_char, c_int) -> c_int;
    type SetVoice = unsafe extern "C" fn(*const c_char) -> c_int;
    type Phonemize =
        unsafe extern "C" fn(*mut *const c_void, c_int, c_int, *mut c_int) -> *const c_char;
    struct Espeak {
        identity: EspeakConfig,
        _library: Library,
        voice: SetVoice,
        phonemize: Phonemize,
    }
    // eSpeak owns global mutable voice/data state, including returned scratch strings.
    // Keep one resident instance and serialize ALL instances through this lock. Never
    // unload/terminate it while another service may hold its global pointers.
    static ESPEAK: Mutex<Option<Espeak>> = Mutex::new(None);
    pub(crate) fn initialize(config: &EspeakConfig) -> Result<()> {
        config.library.read(64 * 1024 * 1024)?;
        if sha256_directory(&config.data_directory)? != config.data_sha256.to_ascii_lowercase() {
            return Err(PiperError::HashMismatch);
        }
        for file in ["phontab", "phonindex", "phondata"] {
            if !config.data_directory.join(file).is_file() {
                return Err(PiperError::Invalid("incomplete eSpeak data directory"));
            }
        }
        let mut identity = config.clone();
        identity.library.path = identity.library.path.canonicalize()?;
        identity.library.sha256.make_ascii_lowercase();
        identity.data_directory = identity.data_directory.canonicalize()?;
        identity.data_sha256.make_ascii_lowercase();
        let mut state = ESPEAK.lock().map_err(|_| PiperError::Worker)?;
        if let Some(existing) = &*state {
            return if existing.identity == identity {
                Ok(())
            } else {
                Err(PiperError::Invalid(
                    "eSpeak library/data identity is process-global and already fixed",
                ))
            };
        }
        let data = CString::new(
            identity
                .data_directory
                .to_str()
                .ok_or(PiperError::Invalid("eSpeak data path must be UTF-8"))?,
        )
        .map_err(|_| PiperError::Invalid("NUL in eSpeak path"))?;
        // SAFETY: caller's trusted library has the pinned eSpeak/piper public C ABI.
        let library = unsafe { Library::new(&identity.library.path) }
            .map_err(|e| PiperError::Native(e.to_string()))?;
        // Resolve every required symbol before initializing global native state.
        let (initialize, voice, phonemize) = unsafe {
            (
                *library
                    .get::<Initialize>(b"espeak_Initialize\0")
                    .map_err(|e| PiperError::Native(e.to_string()))?,
                *library
                    .get::<SetVoice>(b"espeak_SetVoiceByName\0")
                    .map_err(|e| PiperError::Native(e.to_string()))?,
                *library
                    .get::<Phonemize>(b"espeak_TextToPhonemesWithTerminator\0")
                    .map_err(|_| {
                        PiperError::Unsupported(
                            "patched eSpeak TextToPhonemesWithTerminator ABI is required",
                        )
                    })?,
            )
        };
        // SAFETY: valid NUL-terminated owned string; synchronous mode never opens audio.
        // Bit15 tells native code to return an error instead of exit on missing data.
        if unsafe { initialize(2, 0, data.as_ptr(), 1 << 15) } <= 0 {
            return Err(PiperError::Native("eSpeak initialization failed".into()));
        }
        *state = Some(Espeak {
            identity,
            _library: library,
            voice,
            phonemize,
        });
        Ok(())
    }
    pub(crate) fn phonemize(
        text: &str,
        voice: &str,
        max: usize,
        cancel: &Cancellation,
        stop: &Cancellation,
    ) -> Result<Vec<Vec<char>>> {
        let state = loop {
            cancel.check()?;
            stop.check()?;
            match ESPEAK.try_lock() {
                Ok(lock) => break lock,
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(2)),
                Err(_) => return Err(PiperError::Worker),
            }
        };
        let engine = state
            .as_ref()
            .ok_or(PiperError::Invalid("eSpeak not initialized"))?;
        let voice_c = CString::new(voice).map_err(|_| PiperError::Invalid("NUL in voice"))?;
        let text = CString::new(text).map_err(|_| PiperError::Invalid("NUL in text"))?;
        // SAFETY: lock protects native global voice and all returned scratch pointers.
        if unsafe { (engine.voice)(voice_c.as_ptr()) } != 0 {
            return Err(PiperError::Invalid("unknown eSpeak voice"));
        }
        let mut input = text.as_ptr().cast::<c_void>();
        let mut sentences = Vec::new();
        let mut sentence = Vec::new();
        let mut count = 0;
        while !input.is_null() {
            cancel.check()?;
            stop.check()?;
            let before = input;
            let mut terminator = 0;
            // SAFETY: native receives an owned, NUL-terminated UTF-8 allocation and
            // advances its pointer. Returned borrowed string is copied before next call.
            let ptr = unsafe { (engine.phonemize)(&mut input, 1, 2, &mut terminator) };
            if ptr.is_null() {
                return Err(PiperError::Native("null eSpeak phonemes".into()));
            }
            // SAFETY: public ABI guarantees a NUL-terminated scratch string.
            let clause = unsafe { CStr::from_ptr(ptr) }
                .to_str()
                .map_err(|_| PiperError::Native("invalid UTF-8 eSpeak output".into()))?;
            let mut language_flag = false;
            for mut c in normalized(clause, max.saturating_sub(count))? {
                if c == '(' {
                    language_flag = true;
                    continue;
                }
                if language_flag {
                    if c == ')' {
                        language_flag = false;
                    }
                    continue;
                }
                if voice == "pt-br" && c == 'c' {
                    c = 'k';
                }
                count += 1;
                if count > max {
                    return Err(PiperError::TooLarge);
                }
                sentence.push(c);
            }
            let punctuation = match terminator & 0x000f_ffff {
                0x80028 => ".",
                0x82028 => "?",
                0x8302d => "!",
                0x41014 => ", ",
                0x4001e => ": ",
                0x4101e => "; ",
                _ => "",
            };
            for c in punctuation.chars() {
                count += 1;
                if count > max {
                    return Err(PiperError::TooLarge);
                }
                sentence.push(c);
            }
            if terminator & 0x80000 != 0 && !sentence.is_empty() {
                sentences.push(std::mem::take(&mut sentence));
            }
            if input == before {
                return Err(PiperError::Native("eSpeak did not advance input".into()));
            }
        }
        if !sentence.is_empty() {
            sentences.push(sentence);
        }
        Ok(sentences)
    }
}
#[cfg(feature = "espeak-gpl")]
pub(crate) use espeak::{initialize as initialize_espeak, phonemize};
